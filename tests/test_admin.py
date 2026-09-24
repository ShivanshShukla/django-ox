"""
The admin registration: opt-in through django.contrib.admin's autodiscover,
read-only, with the two actions wired to django_ox.actions.
"""

import re
from dataclasses import replace
from datetime import timedelta
from html import unescape
from urllib.parse import parse_qs, urlsplit

import pytest
from django.contrib.auth.models import Permission, User
from django.db import connection
from django.test import Client, RequestFactory
from django.test.utils import CaptureQueriesContext
from django.urls import reverse
from django.utils import timezone

from django_ox import _waiting, metrics
from django_ox.models import OxTask

from .tasks import STATE, add, echo, fail_always


@pytest.fixture
def admin_client(client):
    user = User.objects.create_superuser("ops", "ops@example.com", "pw")
    client.force_login(user)
    return client


def failed_task(worker):
    fail_always.enqueue()
    for _ in range(3):
        OxTask.objects.update(run_after=None)
        worker.run_once()
    db_task = OxTask.objects.get()
    assert db_task.status == OxTask.Status.FAILED
    return db_task


CHANGELIST = "admin:django_ox_oxtask_changelist"
OVERVIEW = "admin:django_ox_oxtask_overview"
OVERVIEW_COLUMNS = (
    "ready",
    "eligible",
    "running",
    "waiting",
    "failed",
    "successful",
    "lost",
    "discarded",
    "oldest",
    "throughput",
    "failure",
    "claim",
)


def overview_rows(body):
    """Each queue's cells from the overview table, keyed by column."""
    rows = {}
    for row in re.findall(r'<tr>\s*<th scope="row">(.*?)</tr>', body, re.S):
        name, rest = row.split("</th>", 1)
        cells = re.findall(r"<td>(.*?)</td>", rest, re.S)
        rows[unescape(name)] = dict(zip(OVERVIEW_COLUMNS, cells, strict=True))
    return rows


def status_link(cell):
    """The change-list path, query and count of one linked status cell."""
    match = re.fullmatch(r'<a href="([^"]*)">([^<]*)</a>', cell)
    assert match, cell
    url = urlsplit(unescape(match.group(1)))
    return url.path, parse_qs(url.query), match.group(2)


@pytest.mark.django_db
class TestChangelist:
    def test_lists_filters_and_searches(self, admin_client, worker):
        failed = failed_task(worker)
        ready = add.enqueue(1, 2)

        response = admin_client.get(reverse(CHANGELIST))
        assert response.status_code == 200
        body = response.content.decode()
        assert "tests.tasks.fail_always" in body
        assert "tests.tasks.add" in body
        assert str(failed.pk) in body
        assert "Retry selected tasks" in body
        assert "Discard selected tasks" in body

        response = admin_client.get(reverse(CHANGELIST), {"status__exact": "FAILED"})
        body = response.content.decode()
        assert "tests.tasks.fail_always" in body
        assert "tests.tasks.add" not in body

        response = admin_client.get(reverse(CHANGELIST), {"q": str(ready.id)})
        body = response.content.decode()
        assert "tests.tasks.add" in body
        assert "tests.tasks.fail_always" not in body

    def test_detail_is_read_only_and_shows_every_traceback(self, admin_client, worker):
        failed = failed_task(worker)
        url = reverse("admin:django_ox_oxtask_change", args=[failed.pk])

        response = admin_client.get(url)
        assert response.status_code == 200
        body = response.content.decode()
        assert body.count("ValueError: boom") == 3
        assert "Attempt 3: builtins.ValueError" in body
        assert 'name="task_path"' not in body  # no editable inputs
        assert 'name="status"' not in body

        # A POST to the change view is refused; the row is not editable.
        response = admin_client.post(url, {"status": "READY"})
        assert response.status_code == 403
        assert OxTask.objects.get().status == OxTask.Status.FAILED

    def test_add_and_delete_are_off(self, admin_client):
        response = admin_client.get(reverse("admin:django_ox_oxtask_add"))
        assert response.status_code == 403

    def test_queue_overview_link_is_in_the_change_list(self, admin_client):
        response = admin_client.get(reverse(CHANGELIST))
        assert response.status_code == 200
        assert f'href="{reverse(OVERVIEW)}"' in response.content.decode()


@pytest.mark.django_db
class TestQueueOverview:
    def _task(self, **over):
        fields = {
            "task_path": "tests.tasks.add",
            "backend_name": "default",
            "queue_name": "default",
            "status": OxTask.Status.READY,
            "enqueued_at": timezone.now(),
        }
        fields.update(over)
        return OxTask.objects.create(**fields)

    def test_direct_url_is_not_an_object_id_redirect(self, admin_client):
        response = admin_client.get(reverse(OVERVIEW), follow=False)
        assert response.status_code == 200
        assert response.request["PATH_INFO"] == reverse(OVERVIEW)

    def test_permissions_and_safe_methods(self, client):
        url = reverse(OVERVIEW)
        assert client.get(url).status_code == 302

        non_staff = User.objects.create_user("member", password="pw")
        client.force_login(non_staff)
        assert client.get(url).status_code == 302

        staff = User.objects.create_user("staff", password="pw", is_staff=True)
        client.force_login(staff)
        assert client.get(url).status_code == 403

        viewer = User.objects.create_user("viewer", password="pw", is_staff=True)
        viewer.user_permissions.add(Permission.objects.get(codename="view_oxtask"))
        client.force_login(viewer)
        assert client.get(url).status_code == 200
        assert client.head(url).status_code == 200

        changer = User.objects.create_user("changer", password="pw", is_staff=True)
        changer.user_permissions.add(Permission.objects.get(codename="change_oxtask"))
        client.force_login(changer)
        assert client.get(url).status_code == 200

        superuser = User.objects.create_superuser("root", "root@example.com", "pw")
        client.force_login(superuser)
        assert client.get(url).status_code == 200

        csrf_client = Client(enforce_csrf_checks=True)
        csrf_client.force_login(superuser)
        csrf_client.cookies["csrftoken"] = "a" * 32
        assert csrf_client.post(url, HTTP_X_CSRFTOKEN="a" * 32).status_code == 405

    def test_rows_metrics_empty_window_and_escaping(self, admin_client):
        now = timezone.now()
        queue = "ops&<critical>"
        self._task(queue_name=queue, status=OxTask.Status.READY)
        self._task(
            queue_name=queue,
            status=OxTask.Status.READY,
            run_after=now + timedelta(hours=1),
        )
        for status in (
            OxTask.Status.RUNNING,
            OxTask.Status.WAITING,
            OxTask.Status.FAILED,
            OxTask.Status.SUCCESSFUL,
            OxTask.Status.LOST,
            OxTask.Status.DISCARDED,
        ):
            self._task(queue_name=queue, status=status)

        body = admin_client.get(reverse(OVERVIEW)).content.decode()
        assert "Queue overview" in body
        assert "ops&amp;&lt;critical&gt;" in body
        assert "ops&<critical>" not in body
        assert "Eligible ready" in body
        assert ">2</a>" in body  # Ready includes the deferred task.
        assert ">1</td>" in body  # Eligible ready does not.
        assert "status__exact=WAITING" in body
        assert "queue_name=ops%26%3Ccritical%3E" in body
        assert "—" in body  # No finished task is inside the five-minute window.
        assert "never" in body

    def test_another_django_ox_permission_is_not_enough(self, client):
        staff = User.objects.create_user("scheduler", password="pw", is_staff=True)
        staff.user_permissions.add(Permission.objects.get(codename="view_oxschedule"))
        client.force_login(staff)
        assert client.get(reverse(OVERVIEW)).status_code == 403

    def test_only_a_permitted_visit_runs_the_aggregate_queries(
        self, client, monkeypatch
    ):
        calls = []
        real_collect = metrics.collect
        monkeypatch.setattr(
            metrics, "collect", lambda **kw: calls.append(kw) or real_collect(**kw)
        )
        url = reverse(OVERVIEW)
        client.get(url)
        staff = User.objects.create_user("staff", password="pw", is_staff=True)
        client.force_login(staff)
        assert client.get(url).status_code == 403
        superuser = User.objects.create_superuser("root", "root@example.com", "pw")
        client.force_login(superuser)
        assert client.get(reverse(CHANGELIST)).status_code == 200
        assert calls == []

        assert client.get(url).status_code == 200
        assert len(calls) == 1

    def test_each_status_links_its_own_count_and_exact_filter(self, admin_client):
        queue = "ops&<x>"
        counts = {
            "READY": 1,
            "RUNNING": 2,
            "WAITING": 3,
            "FAILED": 4,
            "SUCCESSFUL": 5,
            "LOST": 6,
            "DISCARDED": 7,
        }
        for status, count in counts.items():
            for _ in range(count):
                self._task(queue_name=queue, status=status)
        self._task(queue_name=queue, run_after=timezone.now() + timedelta(hours=1))
        counts["READY"] += 1

        body = admin_client.get(reverse(OVERVIEW)).content.decode()
        row = overview_rows(body)[queue]
        for status, count in counts.items():
            path, query, text = status_link(row[status.lower()])
            assert path == reverse(CHANGELIST)
            assert query == {"queue_name": [queue], "status__exact": [status]}
            assert text == str(count)
        assert row["eligible"] == "1"
        assert "No queues have task rows." not in body

    def test_rows_are_ordered_by_queue_name(self, admin_client):
        for name in ("bravo", "alpha", "charlie"):
            self._task(queue_name=name)
        self._task(queue_name="done", status=OxTask.Status.SUCCESSFUL)
        body = admin_client.get(reverse(OVERVIEW)).content.decode()
        assert list(overview_rows(body)) == ["alpha", "bravo", "charlie", "done"]

    def test_finished_window_readings(self, admin_client):
        now = timezone.now()
        for _ in range(3):
            self._task(status=OxTask.Status.SUCCESSFUL, finished_at=now)
        self._task(status=OxTask.Status.FAILED, finished_at=now)
        body = admin_client.get(reverse(OVERVIEW)).content.decode()
        row = overview_rows(body)["default"]
        assert row["throughput"] == "0.80"
        assert row["failure"] == "25.00%"

    def test_an_empty_window_dashes_both_readings(self, admin_client):
        self._task(status=OxTask.Status.RUNNING)
        body = admin_client.get(reverse(OVERVIEW)).content.decode()
        row = overview_rows(body)["default"]
        assert row["throughput"] == "—"
        assert row["failure"] == "—"
        assert row["oldest"] == "—"
        assert row["claim"] == "never"

    def test_ages_carry_units(self, admin_client, monkeypatch):
        now = timezone.now()
        monkeypatch.setattr(timezone, "now", lambda: now)
        self._task(enqueued_at=now - timedelta(seconds=3725))
        self._task(
            status=OxTask.Status.RUNNING, last_attempted_at=now - timedelta(seconds=90)
        )
        self._task(queue_name="skewed", enqueued_at=now + timedelta(minutes=5))
        rows = overview_rows(admin_client.get(reverse(OVERVIEW)).content.decode())
        assert rows["default"]["oldest"] == "1h 2m 5s"
        assert rows["default"]["claim"] == "1m 30s"
        assert rows["skewed"]["oldest"] == "0s"

    def test_a_missing_status_sample_reads_zero(self, admin_client, monkeypatch):
        self._task()
        real_collect = metrics.collect

        def without_waiting(**kw):
            return [
                replace(
                    family,
                    samples=tuple(
                        sample
                        for sample in family.samples
                        if sample[0].get("status") != "waiting"
                    ),
                )
                for family in real_collect(**kw)
            ]

        monkeypatch.setattr(metrics, "collect", without_waiting)
        response = admin_client.get(reverse(OVERVIEW))
        assert response.status_code == 200
        row = overview_rows(response.content.decode())["default"]
        assert status_link(row["waiting"])[2] == "0"

    def test_admin_chrome_notes_and_no_controls(self, admin_client):
        self._task()
        response = admin_client.get(reverse(OVERVIEW))
        body = response.content.decode()
        assert "no-cache" in response["Cache-Control"]
        assert '<div id="user-tools">' in body
        assert "<title>Queue overview |" in body
        assert len(re.findall(r"<h1[^>]*>Queue overview</h1>", body)) == 1
        assert f'<a href="{reverse(CHANGELIST)}">' in body  # breadcrumb
        assert "As of " in body
        assert "trailing five minutes" in body
        assert "Ready includes deferred" in body
        content = body[body.index('<div id="content-main">') :].lower()
        for control in ("<form", "<script", "<button", "http-equiv"):
            assert control not in content

    def test_empty_database_is_a_successful_empty_page(self, admin_client):
        response = admin_client.get(reverse(OVERVIEW))
        assert response.status_code == 200
        assert "No queues have task rows." in response.content.decode()

    def test_collects_five_queries_for_any_number_of_queues(self):
        from django.contrib.admin.sites import AdminSite

        from django_ox.admin import OxTaskAdmin

        class Operator:
            is_active = True
            is_staff = True
            is_superuser = True

            def has_perm(self, _permission):
                return True

            def has_module_perms(self, _app_label):
                return True

        view = OxTaskAdmin(OxTask, AdminSite()).get_urls()[0].callback
        for queues in (8, 16):
            OxTask.objects.all().delete()
            OxTask.objects.bulk_create(
                [
                    OxTask(
                        task_path="tests.tasks.add",
                        backend_name="default",
                        queue_name=f"queue-{number:02}",
                        status=OxTask.Status.READY,
                        enqueued_at=timezone.now(),
                    )
                    for number in range(queues)
                ]
            )
            request = RequestFactory().get("/admin/django_ox/oxtask/overview/")
            request.user = Operator()
            with CaptureQueriesContext(connection) as queries:
                response = view(request)
                response.render()
            task_queries = [
                query
                for query in queries.captured_queries
                if OxTask._meta.db_table in query["sql"]
            ]
            assert len(task_queries) == 5


@pytest.mark.django_db
class TestOtherAdminSites:
    @pytest.fixture(autouse=True)
    def _sites(self, settings):
        settings.ROOT_URLCONF = "tests.urls_admin_sites"

    def test_a_plain_model_admin_change_list_still_renders(self, admin_client):
        response = admin_client.get("/plain/django_ox/oxtask/")
        assert response.status_code == 200
        assert "overview/" not in response.content.decode()

    def test_links_stay_on_the_site_that_serves_the_page(self, admin_client):
        OxTask.objects.create(
            task_path="tests.tasks.add",
            backend_name="default",
            queue_name="default",
            status=OxTask.Status.READY,
            enqueued_at=timezone.now(),
        )
        change_list = admin_client.get("/ops/django_ox/oxtask/").content.decode()
        assert 'href="/ops/django_ox/oxtask/overview/"' in change_list

        overview = admin_client.get("/ops/django_ox/oxtask/overview/").content.decode()
        assert 'href="/ops/django_ox/oxtask/?queue_name=default' in overview
        assert "/admin/" not in overview


@pytest.mark.django_db
class TestActions:
    def test_retry_selected_reports_counts(self, admin_client, worker):
        failed = failed_task(worker)
        ready = add.enqueue(1, 2)

        response = admin_client.post(
            reverse(CHANGELIST),
            {
                "action": "retry_selected",
                "_selected_action": [str(failed.pk), ready.id],
            },
            follow=True,
        )
        messages = [str(m) for m in response.context["messages"]]
        assert "Retried 1 task(s)." in messages
        assert "Skipped 1 task(s) whose status did not allow it." in messages

        failed.refresh_from_db()
        assert failed.status == OxTask.Status.READY
        assert failed.max_attempts == 4

        assert worker.run_once() is True
        failed.refresh_from_db()
        assert failed.attempts == 4

    def test_discard_selected_reports_counts_and_never_runs(self, admin_client, worker):
        claimed_result = add.enqueue(3, 4)
        worker.claim_one()
        ready = add.enqueue(1, 2)

        response = admin_client.post(
            reverse(CHANGELIST),
            {
                "action": "discard_selected",
                "_selected_action": [ready.id, claimed_result.id],
            },
            follow=True,
        )
        messages = [str(m) for m in response.context["messages"]]
        assert "Discarded 1 task(s)." in messages
        assert "Skipped 1 task(s) whose status did not allow it." in messages

        assert OxTask.objects.get(pk=ready.id).status == OxTask.Status.DISCARDED
        assert OxTask.objects.get(pk=claimed_result.id).status == OxTask.Status.RUNNING
        assert worker.run_once() is False
        assert STATE == {}

    def test_actions_need_the_change_permission(self, client):
        user = User.objects.create_user("viewer", password="pw", is_staff=True)
        user.user_permissions.add(Permission.objects.get(codename="view_oxtask"))
        client.force_login(user)
        failed = OxTask.objects.create(
            task_path="tests.tasks.add",
            backend_name="default",
            status=OxTask.Status.FAILED,
            enqueued_at=timezone.now(),
        )

        response = client.get(reverse(CHANGELIST))
        assert response.status_code == 200
        assert "Retry selected tasks" not in response.content.decode()

        response = client.post(
            reverse(CHANGELIST),
            {"action": "retry_selected", "_selected_action": [str(failed.pk)]},
            follow=True,
        )
        assert OxTask.objects.get().status == OxTask.Status.FAILED

    def test_select_across_is_a_few_queries_in_one_transaction(self, admin_client):
        from django.db import connection
        from django.test.utils import CaptureQueriesContext

        now = timezone.now()
        OxTask.objects.bulk_create(
            [
                OxTask(
                    task_path="tests.tasks.add",
                    args=[1, 2],
                    backend_name="default",
                    status=OxTask.Status.READY if i % 2 else OxTask.Status.SUCCESSFUL,
                    enqueued_at=now,
                )
                for i in range(20000)
            ],
            batch_size=1000,
        )
        with CaptureQueriesContext(connection) as ctx:
            response = admin_client.post(
                reverse(CHANGELIST),
                {
                    "action": "discard_selected",
                    "select_across": "1",
                    "index": "0",
                    "_selected_action": [str(OxTask.objects.first().pk)],
                },
                follow=True,
            )
        messages = [str(m) for m in response.context["messages"]]
        assert "Discarded 10000 task(s)." in messages
        assert "Skipped 10000 task(s) whose status did not allow it." in messages
        # 20 UPDATEs for 20,000 rows; the rest is the admin's own requests.
        updates = [q for q in ctx.captured_queries if q["sql"].startswith("UPDATE")]
        assert len(updates) == 20
        assert len(ctx) < 60
        assert OxTask.objects.filter(status=OxTask.Status.DISCARDED).count() == 10000


@pytest.mark.django_db
class TestWaitingRows:
    def test_waiting_is_filtered_displayed_skipped_by_retry_and_discarded(
        self, admin_client
    ):
        held = _waiting.enqueue(add, [1, 2], {}, using="default")
        echo.enqueue("ready")

        body = admin_client.get(reverse(CHANGELIST)).content.decode()
        assert '<td class="field-status">Waiting</td>' in body
        assert "?status__exact=WAITING" in body

        response = admin_client.get(reverse(CHANGELIST), {"status__exact": "WAITING"})
        body = response.content.decode()
        assert "tests.tasks.add" in body
        assert "tests.tasks.echo" not in body

        response = admin_client.post(
            reverse(CHANGELIST),
            {"action": "retry_selected", "_selected_action": [held.id]},
            follow=True,
        )
        messages = [str(m) for m in response.context["messages"]]
        assert "Retried 0 task(s)." in messages
        assert "Skipped 1 task(s) whose status did not allow it." in messages
        assert OxTask.objects.get(pk=held.id).status == OxTask.Status.WAITING

        response = admin_client.post(
            reverse(CHANGELIST),
            {"action": "discard_selected", "_selected_action": [held.id]},
            follow=True,
        )
        messages = [str(m) for m in response.context["messages"]]
        assert "Discarded 1 task(s)." in messages
        assert not any(message.startswith("Skipped") for message in messages)
        assert OxTask.objects.get(pk=held.id).status == OxTask.Status.DISCARDED
