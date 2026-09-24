"""The django-celery-beat import command, which must never write."""

from io import StringIO

import pytest
from django.conf import settings
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import connection

from django_ox.models import OxSchedule

from . import tasks

pytestmark = pytest.mark.django_db(transaction=True)


def make_beat_tables():
    """A minimal stand-in for the tables django-celery-beat creates."""
    dt_type = connection.data_types.get("DateTimeField", "datetime")
    with connection.cursor() as cursor:
        cursor.execute(
            "CREATE TABLE django_celery_beat_crontabschedule ("
            "id integer primary key, minute varchar(64), hour varchar(64), "
            "day_of_month varchar(64), month_of_year varchar(64), "
            "day_of_week varchar(64), timezone varchar(63))"
        )
        cursor.execute(
            "CREATE TABLE django_celery_beat_intervalschedule ("
            "id integer primary key, every integer, period varchar(24))"
        )
        cursor.execute(
            f"CREATE TABLE django_celery_beat_periodictask ("
            f"id integer primary key, name varchar(200), task varchar(200), "
            f"args text, kwargs text, queue varchar(200), enabled boolean, "
            f"crontab_id integer, interval_id integer, one_off boolean, "
            f"start_time {dt_type}, expires {dt_type})"
        )
        cursor.execute(
            "INSERT INTO django_celery_beat_crontabschedule "
            "(id, minute, hour, day_of_month, month_of_year, day_of_week, "
            "timezone) VALUES (1, %s, %s, %s, %s, %s, %s)",
            ["0", "2", "*", "*", "*", settings.TIME_ZONE],
        )
        cursor.execute(
            "INSERT INTO django_celery_beat_intervalschedule (id, every, period) "
            "VALUES (1, %s, %s)",
            [90, "minutes"],
        )
        # Parameterised, and booleans passed as booleans: PostgreSQL will not
        # accept 1 for a boolean column where SQLite and MySQL both would.
        rows = [
            (
                1,
                "nightly",
                "reports.tasks.daily",
                None,
                True,
                1,
                None,
                False,
                None,
                None,
            ),
            (
                2,
                "poller",
                "mail.tasks.poll",
                "mail",
                True,
                None,
                1,
                False,
                None,
                None,
            ),
            (3, "orphan", "x.y.z", None, True, None, None, False, None, None),
        ]
        for (
            pk,
            name,
            task,
            queue,
            enabled,
            crontab_id,
            interval_id,
            one_off,
            start_time,
            expires,
        ) in rows:
            cursor.execute(
                "INSERT INTO django_celery_beat_periodictask "
                "(id, name, task, args, kwargs, queue, enabled, crontab_id, "
                "interval_id, one_off, start_time, expires) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                [
                    pk,
                    name,
                    task,
                    "[]",
                    "{}",
                    queue,
                    enabled,
                    crontab_id,
                    interval_id,
                    one_off,
                    start_time,
                    expires,
                ],
            )


def drop_beat_tables():
    with connection.cursor() as cursor:
        for table in (
            "django_celery_beat_periodictask",
            "django_celery_beat_crontabschedule",
            "django_celery_beat_intervalschedule",
        ):
            cursor.execute(f"DROP TABLE IF EXISTS {table}")


@pytest.fixture
def beat_tables():
    make_beat_tables()
    yield
    drop_beat_tables()


def run():
    out = StringIO()
    call_command("ox_import_beat_schedules", stdout=out)
    return out.getvalue()


def test_it_writes_nothing(beat_tables):
    # The claim the command's own docstring makes. A migration is a decision
    # about production timing, so it prints and stops.
    run()
    assert OxSchedule.objects.count() == 0


def test_it_prints_the_allow_list_and_the_calls(beat_tables):
    output = run()
    assert '"reports.tasks.daily": "reports.tasks.daily"' in output
    assert 'cron="0 2 * * *"' in output
    assert "every_seconds=5400" in output


def test_the_generated_calls_actually_run(beat_tables, monkeypatch):
    """
    Execute the output rather than matching strings in it.

    The previous version of this test asserted the presence of
    `queue_name="mail"`, which named a field the model does not have, so it
    pinned an output that raised TypeError the moment anyone pasted it. A
    printed migration is only worth printing if it runs.
    """
    from datetime import datetime

    from django_ox.registry import ScheduleKind, register

    monkeypatch.setattr("django_ox.registry._registry", {})
    monkeypatch.setattr("django_ox.registry._discovered", True)
    for key in ("reports.tasks.daily", "mail.tasks.poll"):
        register(ScheduleKind(key=key, task=tasks.add))

    output = run()
    assert "# 2. Create the schedules." in output
    section_2 = output.split("# 2. Create the schedules.")[1]
    calls = [
        line for line in section_2.splitlines() if line.startswith("create_schedule(")
    ]
    assert calls, "the command printed no calls to check"

    from django_ox.stored import create_schedule

    for call in calls:
        eval(call, {"create_schedule": create_schedule, "datetime": datetime})  # noqa: S307

    assert OxSchedule.objects.count() == len(calls)


def test_a_row_with_positional_arguments_is_not_translated(beat_tables):
    # A stored schedule takes keyword arguments only, so a beat row carrying
    # positional args cannot be expressed and must not be printed as if it can.
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET args = %s WHERE id = 1",
            ['["emea"]'],
        )
    output = run()
    assert "nightly" not in [
        line.split("name=")[1].split(",")[0].strip("'\"")
        for line in output.splitlines()
        if line.startswith("create_schedule(")
    ]
    assert "positional arguments" in output


def test_keyword_arguments_are_carried_over(beat_tables):
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET kwargs = %s WHERE id = 1",
            ['{"region": "emea"}'],
        )
    assert "arguments={'region': 'emea'}" in run()


def test_it_explains_what_it_could_not_translate(beat_tables):
    output = run()
    assert "orphan" in output
    assert "Not translated" in output


def test_it_warns_that_interval_timing_differs(beat_tables):
    # The difference most likely to surprise someone migrating.
    output = run()
    assert "fixed instant" in output


def test_a_missing_table_is_an_error_not_an_empty_run(beat_tables):
    drop_beat_tables()
    with pytest.raises(CommandError, match="No django_celery_beat_periodictask"):
        run()
    make_beat_tables()  # so the fixture's teardown is symmetric


def test_a_schedule_in_another_timezone_is_not_translated(beat_tables):
    # A stored schedule has no zone of its own, so a beat schedule carrying
    # one would run at a different time. The command names the difference
    # rather than emitting a line that quietly means something else.
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_crontabschedule SET timezone = %s WHERE id = 1",
            ["Asia/Tokyo"],
        )
    output = run()
    assert "Asia/Tokyo" in output
    assert "nightly" not in [
        line.split("name=")[1].split(",")[0].strip("'\"")
        for line in output.splitlines()
        if line.startswith("create_schedule(")
    ]


def test_an_equivalent_zone_under_another_name_is_still_translated(beat_tables):
    # US/Eastern and America/New_York are one zone. Comparing the strings
    # would divert a correctly-aligned schedule into the untranslated list.
    from django.test import override_settings

    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_crontabschedule SET timezone = %s WHERE id = 1",
            ["US/Eastern"],
        )
    with override_settings(TIME_ZONE="America/New_York"):
        output = run()
    assert "US/Eastern" not in output
    assert any(
        line.startswith("create_schedule(") and "nightly" in line
        for line in output.splitlines()
    )


def test_a_name_holding_a_quote_still_emits_runnable_code(beat_tables):
    # The output is meant to be pasted, so a name that breaks the literal
    # is a line that does not parse.
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET name = %s WHERE id = 1",
            ['say "hello"\\backslash'],
        )
    calls = [line for line in run().splitlines() if line.startswith("create_schedule(")]
    assert calls, "the command printed no calls to check"
    for line in calls:
        compile(line, "<generated>", "eval")


def test_a_database_it_cannot_reach_is_a_sentence(monkeypatch):
    """
    This command prints code for a person to read and paste. A driver
    traceback in the middle of that is nothing they can act on, and its
    three siblings report the same failure in one line.
    """
    from django.db import OperationalError, connections

    def refuse(*args, **kwargs):
        raise OperationalError("could not connect to server")

    monkeypatch.setattr(connections["default"].introspection, "table_names", refuse)
    with pytest.raises(CommandError) as caught:
        call_command("ox_import_beat_schedules")
    assert "Database unreachable: could not connect to server" in str(caught.value)


def test_one_off_task_is_not_translated(beat_tables):
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET one_off = %s WHERE id = 1",
            [True],
        )
    output = run()
    assert "nightly" not in [
        line.split("name=")[1].split(",")[0].strip("'\"")
        for line in output.splitlines()
        if line.startswith("create_schedule(")
    ]
    assert "one-off tasks have no equivalent" in output


def test_expired_task_is_not_translated(beat_tables):
    past_iso = "2020-01-01 00:00:00"
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET expires = %s WHERE id = 1",
            [past_iso],
        )
    output = run()
    assert "nightly" not in [
        line.split("name=")[1].split(",")[0].strip("'\"")
        for line in output.splitlines()
        if line.startswith("create_schedule(")
    ]
    assert "it has expired" in output


def test_expiry_at_or_before_future_start_is_not_translated(beat_tables):
    future_start = "2099-01-02 00:00:00"
    earlier_expiry = "2099-01-01 00:00:00"
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET start_time = %s, "
            "expires = %s WHERE id = 1",
            [future_start, earlier_expiry],
        )
    output = run()
    assert "nightly" not in [
        line.split("name=")[1].split(",")[0].strip("'\"")
        for line in output.splitlines()
        if line.startswith("create_schedule(")
    ]
    assert "its expiry is at or before its start time" in output


def test_future_start_and_expiry_are_preserved(beat_tables, monkeypatch):
    from datetime import datetime

    from django.utils import timezone

    from django_ox.registry import ScheduleKind, register

    monkeypatch.setattr("django_ox.registry._registry", {})
    monkeypatch.setattr("django_ox.registry._discovered", True)
    for key in ("reports.tasks.daily", "mail.tasks.poll"):
        register(ScheduleKind(key=key, task=tasks.add))

    future_start = "2099-01-01 10:00:00"
    future_expiry = "2099-12-31 10:00:00"
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET start_time = %s, "
            "expires = %s WHERE id = 1",
            [future_start, future_expiry],
        )

    output = run()
    assert "start_time=datetime.fromisoformat(" in output
    assert "end_time=datetime.fromisoformat(" in output

    section_2 = output.split("# 2. Create the schedules.")[1]
    calls = [
        line for line in section_2.splitlines() if line.startswith("create_schedule(")
    ]

    from django_ox.stored import create_schedule

    for call in calls:
        eval(call, {"create_schedule": create_schedule, "datetime": datetime})  # noqa: S307

    schedule = OxSchedule.objects.get(name="nightly")
    if settings.USE_TZ:
        expected_start = timezone.make_aware(
            datetime.fromisoformat(future_start), connection.timezone
        )
        expected_end = timezone.make_aware(
            datetime.fromisoformat(future_expiry), connection.timezone
        )
    else:
        expected_start = datetime.fromisoformat(future_start)
        expected_end = datetime.fromisoformat(future_expiry)

    assert schedule.start_time == expected_start
    assert schedule.end_time == expected_end


def test_past_start_is_omitted(beat_tables):
    past_start = "2020-01-01 00:00:00"
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET start_time = %s WHERE id = 1",
            [past_start],
        )
    output = run()
    nightly_call = next(
        line
        for line in output.splitlines()
        if line.startswith("create_schedule(") and "nightly" in line
    )
    assert "start_time=" not in nightly_call


def test_old_table_without_new_columns(beat_tables):
    """
    An older django-celery-beat table without one_off, start_time, or expires
    must still be imported cleanly.
    """
    with connection.cursor() as cursor:
        cursor.execute(
            "ALTER TABLE django_celery_beat_periodictask DROP COLUMN one_off"
        )
        cursor.execute(
            "ALTER TABLE django_celery_beat_periodictask DROP COLUMN start_time"
        )
        cursor.execute(
            "ALTER TABLE django_celery_beat_periodictask DROP COLUMN expires"
        )

    output = run()
    assert 'cron="0 2 * * *"' in output
    assert "every_seconds=5400" in output


def test_disabled_task_remains_disabled(beat_tables):
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET enabled = %s WHERE id = 1",
            [False],
        )
    output = run()
    assert "enabled=False" in output


def test_timezone_handling_use_tz(beat_tables):
    from django.test import override_settings

    future_start = "2099-05-01 12:00:00"
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET start_time = %s WHERE id = 2",
            [future_start],
        )
    with override_settings(USE_TZ=True, TIME_ZONE="America/New_York"):
        output = run()
        assert "start_time=datetime.fromisoformat(" in output

    with override_settings(USE_TZ=False, TIME_ZONE="America/New_York"):
        output = run()
        assert "start_time=datetime.fromisoformat(" in output
