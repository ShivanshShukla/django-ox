"""
`--batch` under contention: a claim that lost every race found a busy queue,
not an empty one.

Only the compare-and-set claim can come back empty while rows are due. It
reads CLAIM_BATCH_SIZE candidates, and another worker can take every one of
them before its UPDATEs land, with more rows due behind them. SQLite always
claims that way; the in-process tests force it on every database. The rival's
claims are the shipped claim, landing in the gap between the read and the
compare-and-set, so nothing here depends on load or on timing.
"""

import json
import logging
from collections import Counter

import pytest
from django.db import connections

from django_ox.models import OxTask
from django_ox.worker import CLAIM_BATCH_SIZE, Worker

from .conftest import start_worker_thread, wait_for
from .tasks import echo
from .test_batch_max_tasks import finish
from .test_supervisor import child_env, start_worker
from .test_worker import reap_away

pytestmark = pytest.mark.django_db(transaction=True)

LEFT_BEHIND = 3


def events(caplog, name):
    return [r for r in caplog.records if getattr(r, "event", None) == name]


@pytest.fixture
def compare_and_set(monkeypatch):
    """
    Claim by compare-and-set on every database and on every thread.

    A worker thread opens a connection of its own, with features of its own,
    so the class is patched as well as this thread's instance. The instance
    goes first: patched after the class, its undo would record the patched
    value as the original and leave this thread on the wrong path for good.
    """
    features = connections["default"].features
    monkeypatch.setattr(features, "has_select_for_update_skip_locked", False)
    monkeypatch.setattr(type(features), "has_select_for_update_skip_locked", False)


def enqueue_in_claim_order(count):
    # Distinct priorities make the claim order total, so the rival's claims
    # take exactly the rows the worker under test read, on any database.
    return [echo.using(priority=count - i).enqueue(i) for i in range(count)]


class _RivalTakesWhatIsRead:
    """The candidate slice, with a rival claiming each row once it is read."""

    def __init__(self, queryset, worker):
        self.queryset = queryset
        self.worker = worker

    def __getitem__(self, index):
        rows = list(self.queryset[index])
        self.worker.read.extend(row.pk for row in rows)
        for _ in rows:
            self.worker.stolen.append(self.worker.rival.claim_one().pk)
        return rows


class LosesItsFirstRead(Worker):
    """
    A worker whose first candidate read goes, row by row, to a rival.

    Also the WORKER_CLASS of the command test below, so it takes whatever the
    command passes.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.rival = Worker(poll_interval=self.poll_interval)
        self.read = []
        self.stolen = []

    def _ready_queryset(self):
        queryset = super()._ready_queryset()
        if self.read:
            return queryset
        return _RivalTakesWhatIsRead(queryset, self)


class RefusesAfterALostRace(LosesItsFirstRead):
    """
    Calls the base claim once, which loses every race, then refuses without
    calling it, the way an override enforcing a limit of its own might.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.refusals = 0

    def claim_one(self):
        if not self.read:
            return super().claim_one()
        self.refusals += 1
        return None


class TestTheClaimSaysItLostEveryRace:
    def test_a_read_that_went_to_a_rival(self, compare_and_set):
        enqueue_in_claim_order(CLAIM_BATCH_SIZE + LEFT_BEHIND)
        worker = LosesItsFirstRead(poll_interval=0.05)

        assert worker.claim_one() is None
        assert len(worker.read) == CLAIM_BATCH_SIZE
        assert sorted(worker.stolen) == sorted(worker.read)
        assert worker._claim_contended is True
        assert OxTask.objects.filter(status=OxTask.Status.READY).count() == (
            LEFT_BEHIND
        )

    def test_a_row_reaped_before_its_read_back(self, compare_and_set, monkeypatch):
        echo.enqueue("only")
        worker = Worker(poll_interval=0.05)
        rival = Worker(poll_interval=0.05)
        real_reload = Worker._reload_claimed
        stolen = []

        def reload_after_theft(self, pk, granted_epoch):
            if self is worker and not stolen:
                reap_away(worker, OxTask.objects.get(pk=pk))
                stolen.append(rival.claim_one())
            return real_reload(self, pk, granted_epoch)

        monkeypatch.setattr(Worker, "_reload_claimed", reload_after_theft)

        assert worker.claim_one() is None
        assert stolen[0] is not None, "the rival never got the row"
        assert worker._claim_contended is True

    def test_an_empty_read_is_not_contention(self, compare_and_set):
        worker = Worker(poll_interval=0.05)

        assert worker.claim_one() is None
        assert worker._claim_contended is False


class TestABatchOutlastsALostRace:
    def test_it_runs_what_the_rival_left(self, compare_and_set, caplog):
        results = enqueue_in_claim_order(CLAIM_BATCH_SIZE + LEFT_BEHIND)
        worker = LosesItsFirstRead(poll_interval=0.05, batch=True)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            thread = start_worker_thread(worker)
            thread.join(timeout=30)
        assert not thread.is_alive(), "the batch worker never finished"

        stolen = set(worker.stolen)
        assert len(stolen) == CLAIM_BATCH_SIZE
        for result in results:
            row = OxTask.objects.get(id=result.id)
            if row.pk in stolen:
                assert row.status == OxTask.Status.RUNNING
                assert row.locked_by == worker.rival.worker_id
            else:
                assert row.status == OxTask.Status.SUCCESSFUL, (
                    "the batch ended on a lost race with this row still due"
                )
        (done,) = events(caplog, "worker_batch_empty")
        assert done.claimed == LEFT_BEHIND

    def test_a_refusal_after_it_still_ends_the_batch(self, compare_and_set, caplog):
        """
        The flag belongs to one claim. An override that refuses without
        calling the base claim must not inherit it from an earlier one, or
        the batch polls for as long as the refusals last.
        """
        enqueue_in_claim_order(CLAIM_BATCH_SIZE + LEFT_BEHIND)
        worker = RefusesAfterALostRace(poll_interval=0.05, batch=True)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            thread = start_worker_thread(worker)
            assert wait_for(
                lambda: not thread.is_alive() or worker.refusals > 1, timeout=30
            )
        assert worker.stolen, "the first claim did not lose its race"
        assert not thread.is_alive(), "a lost race outlived the claim that lost it"
        assert worker.refusals == 1
        (done,) = events(caplog, "worker_batch_empty")
        assert done.claimed == 0


def test_the_command_runs_what_a_lost_race_left(tmp_path):
    """
    The real command, on the one database that claims this way in
    production. The rival lives in the worker process and never runs what
    it claims, so its rows stay RUNNING and everything else must finish.
    """
    if connections["default"].features.has_select_for_update_skip_locked:
        pytest.skip("this database claims with SKIP LOCKED, not compare-and-set")
    enqueue_in_claim_order(CLAIM_BATCH_SIZE + LEFT_BEHIND)
    env = child_env()
    env["OX_TEST_TASKS_OPTIONS"] = json.dumps(
        {"WORKER_CLASS": "tests.test_batch_contention.LosesItsFirstRead"}
    )
    log = tmp_path / "worker.log"
    proc = start_worker(tmp_path, "--batch", "--interval", "0.05", env=env)
    assert finish(proc, log) == 0, log.read_text()
    assert Counter(OxTask.objects.values_list("status", flat=True)) == {
        OxTask.Status.RUNNING: CLAIM_BATCH_SIZE,
        OxTask.Status.SUCCESSFUL: LEFT_BEHIND,
    }, log.read_text()
    assert f"found nothing to claim after {LEFT_BEHIND} claim(s)" in log.read_text()
