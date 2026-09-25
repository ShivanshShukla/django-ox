"""The django-celery-beat import command, which must never write."""

import ast
import code
import json
import sys
import tokenize
from contextlib import contextmanager, nullcontext
from datetime import UTC, datetime, timedelta, tzinfo
from io import StringIO

import pytest
from django.conf import settings
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import DatabaseError, connection, connections
from django.test import override_settings
from django.utils import timezone

from django_ox.models import OxSchedule, OxScheduleTick

from . import tasks

pytestmark = pytest.mark.django_db(transaction=True)


#: The periodictask columns django-celery-beat added after its first table:
#: expires is in 0001, one_off and start_time arrived in 0007.
LATER_COLUMNS = ("one_off", "start_time", "expires")


def make_beat_tables(columns=LATER_COLUMNS, *, db="default"):
    """
    A minimal stand-in for the tables django-celery-beat creates.

    columns picks which of the later periodictask columns exist, so an older
    table is built in its own shape. The test tables are created directly
    rather than reduced with DROP COLUMN, which SQLite added in 3.35.0 and
    Django enables from 3.35.5.
    """
    datetime_type = connections[db].data_types["DateTimeField"]
    types = {"one_off": "boolean", "start_time": datetime_type}
    later = "".join(f", {name} {types.get(name, datetime_type)}" for name in columns)
    with connections[db].cursor() as cursor:
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
            "CREATE TABLE django_celery_beat_periodictask ("
            "id integer primary key, name varchar(200), task varchar(200), "
            "args text, kwargs text, queue varchar(200), enabled boolean, "
            f"crontab_id integer, interval_id integer{later})"
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
    one_off = {"one_off": False} if "one_off" in columns else {}
    insert_task(1, "nightly", "reports.tasks.daily", db=db, crontab_id=1, **one_off)
    insert_task(
        2, "poller", "mail.tasks.poll", db=db, queue="mail", interval_id=1, **one_off
    )
    insert_task(3, "orphan", "x.y.z", db=db, **one_off)


def insert_task(pk, name, task, *, db="default", **columns):
    """
    Add one beat row. Columns not given are NULL, or empty for the arguments.

    Parameterised, and booleans passed as booleans: PostgreSQL will not
    accept 1 for a boolean column where SQLite and MySQL both would.
    """
    values = {"args": "[]", "kwargs": "{}", "enabled": True, **columns}
    names = ", ".join(["id", "name", "task", *values])
    marks = ", ".join(["%s"] * (len(values) + 3))
    with connections[db].cursor() as cursor:
        cursor.execute(
            f"INSERT INTO django_celery_beat_periodictask ({names}) "  # noqa: S608
            f"VALUES ({marks})",
            [pk, name, task, *values.values()],
        )


def drop_beat_tables(db="default"):
    with connections[db].cursor() as cursor:
        for table in (
            "django_celery_beat_periodictask",
            "django_celery_beat_crontabschedule",
            "django_celery_beat_intervalschedule",
        ):
            cursor.execute(f"DROP TABLE IF EXISTS {table}")


@pytest.fixture
def beat_tables():
    # Created inside the try: a setup that fails halfway still drops what
    # it made, and dropping skips a table that was never created.
    try:
        make_beat_tables()
        yield
    finally:
        drop_beat_tables()


def run():
    out = StringIO()
    call_command("ox_import_beat_schedules", stdout=out)
    return out.getvalue()


SECTION_2 = "# 2. Create the schedules."


def section_1(output):
    """The settings fragment, from its first key to section 2."""
    return output[output.index('"SCHEDULABLE_TASKS": {') : output.index(SECTION_2)]


def section_2(output):
    """What is pasted as code: section 2's heading to the end of the output."""
    return output[output.index(SECTION_2) :]


def run_as_module(section):
    """Section 2 as a data migration or a script runs it: compiled whole."""
    namespace = {}
    exec(compile(section, "<section 2>", "exec"), namespace)  # noqa: S102
    return namespace


class _Shell(code.InteractiveConsole):
    """A manage.py shell that records what went wrong instead of printing it."""

    def __init__(self, namespace):
        super().__init__(locals=namespace)
        self.errors = []

    def showtraceback(self):
        self.errors.append(sys.exc_info()[1])

    def showsyntaxerror(self, *args, **kwargs):
        self.errors.append(sys.exc_info()[1])


def paste_into_shell(section):
    """Section 2 as someone pastes it into a shell: one line at a time."""
    namespace = {}
    shell = _Shell(namespace)
    # splitlines, not split("\n"): it breaks wherever a terminal or an editor
    # may end a line, at CR and U+2028 among others.
    for line in section.splitlines():
        shell.push(line)
    shell.push("")
    assert not shell.errors, shell.errors
    return namespace


def literal_after(text, prefix):
    """The Python literal that starts right after prefix in text, evaluated."""
    rest = text[text.index(prefix) + len(prefix) :]
    token = next(tokenize.generate_tokens(StringIO(rest).readline))
    return ast.literal_eval(token.string)


@pytest.fixture
def recorded(monkeypatch):
    """create_schedule as section 2 imports it, recording its calls instead."""
    calls = []
    monkeypatch.setattr(
        "django_ox.stored.create_schedule", lambda **fields: calls.append(fields)
    )
    return calls


def calls_by_name(output, recorded):
    """Section 2 run with create_schedule recorded: each call's fields by name."""
    recorded.clear()
    run_as_module(section_2(output))
    return {call["name"]: call for call in recorded}


@pytest.fixture
def schedulable(monkeypatch):
    """The fixture's two translatable task paths, registered as schedulable."""
    from django_ox.registry import ScheduleKind, register

    monkeypatch.setattr("django_ox.registry._registry", {})
    monkeypatch.setattr("django_ox.registry._discovered", True)
    for key in ("reports.tasks.daily", "mail.tasks.poll"):
        register(ScheduleKind(key=key, task=tasks.add))


def instant(text):
    """A datetime written as naive text, as the command reads it back."""
    value = datetime.fromisoformat(text)
    if settings.USE_TZ:
        return timezone.make_aware(value, connection.timezone)
    return value


@pytest.fixture
def clock(monkeypatch):
    """
    Pin timezone.now(), which the command and create_schedule both read.

    Returns a setter taking the time as naive text, and the list of the
    instants each reading of the clock returned.
    """
    readings = []

    def pin(text):
        at = instant(text)

        def now():
            readings.append(at)
            return at

        monkeypatch.setattr(timezone, "now", now)
        return at

    pin.readings = readings
    return pin


def test_it_writes_nothing(beat_tables):
    # The claim the command's own docstring makes. A migration is a decision
    # about production timing, so it prints and stops.
    run()
    assert OxSchedule.objects.count() == 0


def test_it_prints_the_allow_list_and_the_calls(beat_tables):
    output = run()
    assert "'reports.tasks.daily': 'reports.tasks.daily'" in output
    assert "cron='0 2 * * *'" in output
    assert "every_seconds=5400" in output


def test_the_generated_calls_actually_run(beat_tables, schedulable):
    """
    Execute the output rather than matching strings in it.

    The previous version of this test asserted the presence of
    `queue_name="mail"`, which named a field the model does not have, so it
    pinned an output that raised TypeError the moment anyone pasted it. A
    printed migration is only worth printing if it runs.
    """
    output = run()
    calls = [
        line for line in output.splitlines() if line.startswith("create_schedule(")
    ]
    assert calls, "the command printed no calls to check"

    # As pasted: on section 2's own imports, in a namespace of its own.
    run_as_module(section_2(output))

    assert OxSchedule.objects.count() == len(calls)


def test_a_row_with_positional_arguments_is_not_translated(beat_tables, recorded):
    # A stored schedule takes keyword arguments only, so a beat row carrying
    # positional args cannot be expressed and must not be printed as if it can.
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET args = %s WHERE id = 1",
            ['["emea"]'],
        )
    output = run()
    assert "nightly" not in calls_by_name(output, recorded)
    assert (
        "#   'nightly': it passes positional arguments, and a stored schedule "
        "takes keyword arguments only; rewrite the task signature or the row"
        in output.splitlines()
    )


def test_keyword_arguments_are_carried_over(beat_tables):
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET kwargs = %s WHERE id = 1",
            ['{"region": "emea"}'],
        )
    assert "arguments={'region': 'emea'}" in run()


def test_it_explains_what_it_could_not_translate(beat_tables):
    # Each reason on its own row's line. These two are the fallbacks every
    # other check runs before, so a check that matched too much would
    # shadow them.
    insert_task(4, "dangling", "x.y.z", crontab_id=99)
    lines = run().splitlines()
    assert "# Not translated, and why:" in lines
    assert "#   'orphan': solar and clocked schedules have no equivalent" in lines
    assert "#   'dangling': its schedule row is missing" in lines


def test_it_warns_that_interval_timing_differs(beat_tables):
    # The difference most likely to surprise someone migrating.
    output = run()
    assert "fixed instant" in output


def test_it_ends_with_the_whole_note_on_applying(beat_tables):
    # Keep the complete application note under regression coverage.
    assert run().endswith(
        "\n\n"
        "# Read before applying. Intervals are counted from a fixed instant here,\n"
        "# not from the last run, so their fire times will differ from Celery's.\n"
        "# Schedules are created with their enabled state preserved. Unless a\n"
        "# start time is supplied, they start when created. If a printed start\n"
        "# time passes before applying, the latest missed tick may run immediately.\n"
        "# Enabling a disabled schedule resets its start time; check future starts\n"
        "# before enabling.\n"
        "# Start and expiry bounds are imported, with beat's exclusive expiry\n"
        "# represented by an inclusive end_time one microsecond earlier. With\n"
        "# USE_TZ=True, date bounds preserve their instants. With USE_TZ=False, date\n"
        "# bounds are emitted as naive local times, not reinterpreted as UTC. This\n"
        "# differs from django-celery-beat 2.9.0 with\n"
        "# DJANGO_CELERY_BEAT_TZ_AWARE=False, which compares stored naive bounds\n"
        "# against UTC wall time; review these bounds before applying the output.\n"
        "# For a task that has never run and has a start_time, django-celery-beat\n"
        "# 2.9.0 can run it once as soon as that time is reached or first observed\n"
        "# after it has passed, then follow its schedule. The imported schedule does\n"
        "# not reproduce that initial catch-up run; it waits for its first scheduled\n"
        "# tick at or after start_time.\n"
        "# If an expiry passes before applying, creation may fail or leave a\n"
        "# schedule that never runs.\n"
        "# Regenerate stale output before applying. After a partial application,\n"
        "# inspect existing schedules and apply only the remaining calls; do not\n"
        "# paste the whole output again.\n"
        "# A queue or priority set on a beat task has no equivalent on a stored\n"
        "# schedule; set it on the task.\n"
    )


def note(output):
    """The note after the calls, as one line of prose."""
    lines = output[output.index("# Read before applying.") :].splitlines()
    return " ".join(line.removeprefix("# ") for line in lines)


def test_it_warns_that_beats_first_run_at_a_start_time_is_not_reproduced(
    beat_tables, recorded
):
    # Beat runs a task that has never run once at its start_time, off the
    # schedule's ticks. The note says so, and the call carries the schedule
    # as it is, with no extra run or changed cron to imitate it.
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET start_time = %s WHERE id = 1",
            ["2099-01-01 10:00:00"],
        )
    output = run()
    calls = calls_by_name(output, recorded)
    assert (
        "For a task that has never run and has a start_time, django-celery-beat "
        "2.9.0 can run it once as soon as that time is reached or first observed "
        "after it has passed, then follow its schedule. The imported schedule "
        "does not reproduce that initial catch-up run; it waits for its first "
        "scheduled tick at or after start_time."
    ) in note(output)
    assert sorted(call["name"] for call in recorded) == ["nightly", "poller"]
    assert calls["nightly"]["cron"] == "0 2 * * *"
    assert calls["nightly"]["start_time"] == instant("2099-01-01 10:00:00")


def test_a_missing_table_is_an_error_not_an_empty_run(beat_tables):
    drop_beat_tables()
    with pytest.raises(CommandError, match="No django_celery_beat_periodictask"):
        run()
    make_beat_tables()  # so the fixture's teardown is symmetric


def test_a_schedule_in_another_timezone_is_not_translated(beat_tables, recorded):
    # A stored schedule has no zone of its own, so a beat schedule carrying
    # one would run at a different time. The command names the difference
    # rather than emitting a line that quietly means something else.
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_crontabschedule SET timezone = %s WHERE id = 1",
            ["Asia/Tokyo"],
        )
    output = run()
    assert "nightly" not in calls_by_name(output, recorded)
    assert (
        "#   'nightly': its schedule runs in 'Asia/Tokyo', and a stored schedule "
        f"has no zone of its own: it would run in {settings.TIME_ZONE!a}, at a "
        "different time" in output.splitlines()
    )


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


READ_ERROR = (
    "Cannot read beat schedules from database 'default'; check database access "
    "and stored values."
)

#: Stored dates some driver cannot hand back as a date, by database.
UNREADABLE = {
    "postgresql": ["infinity", "-infinity"],
    "sqlite": ["2099-13-45 00:00:00"],
    "mysql": ["0000-00-00 00:00:00"],
}


def import_fails(**options):
    """Run the command expecting it to stop: its one line, and what it printed."""
    out = StringIO()
    with pytest.raises(CommandError) as caught:
        call_command("ox_import_beat_schedules", stdout=out, **options)
    return caught.value, out.getvalue()


def decoded_by_driver(column):
    """What the driver hands back for row 1's column, or what it raises."""
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT {column} FROM django_celery_beat_periodictask "  # noqa: S608
                "WHERE id = 1"
            )
            return cursor.fetchone()[0]
    except (DatabaseError, ValueError, OverflowError) as exc:
        return exc


@pytest.mark.parametrize("column", ["start_time", "expires"])
def test_a_date_the_driver_cannot_read_stops_the_import_in_one_line(
    beat_tables, column
):
    """
    PostgreSQL's 'infinity' and '-infinity', an impossible date kept as
    text on SQLite, and MySQL's zero date. What reaches the command depends
    on the driver: psycopg raises while the rows are read, where no single
    row can be set aside; PyMySQL hands back text that is no date; and
    mysqlclient hands back None for a value that is not NULL. Each stops
    the import with one line, before printing anything. A driver that
    decodes the value as a date, as psycopg2 does with infinity, has raised
    no read error, and that value is not checked here.
    """
    checked = []
    for stored in UNREADABLE[connection.vendor]:
        with connection.cursor() as cursor:
            cursor.execute(
                f"UPDATE django_celery_beat_periodictask SET {column} = %s "  # noqa: S608
                "WHERE id = 1",
                [stored],
            )
        if isinstance(decoded_by_driver(column), datetime):
            continue
        error, printed = import_fails()
        assert str(error) == READ_ERROR
        assert printed == ""
        checked.append(stored)
    if not checked:
        pytest.skip(
            f"this driver reads each of {UNREADABLE[connection.vendor]} as a date"
        )


@pytest.mark.parametrize("error", [DatabaseError, ValueError, OverflowError])
def test_any_failure_to_read_stops_the_import_in_one_line(
    beat_tables, monkeypatch, error
):
    """
    The handler itself, on every backend and whatever the driver does:
    database, decoding and conversion failures while the rows are read all
    end in the same line, which names no driver detail.
    """
    from django_ox.management.commands.ox_import_beat_schedules import Command

    def fail(*args):
        raise error("a driver detail")

    monkeypatch.setattr(Command, "_parse_datetime", staticmethod(fail))
    failure, printed = import_fails()
    assert str(failure) == READ_ERROR
    assert type(failure.__cause__) is error
    assert printed == ""


def test_an_expiry_in_year_one_stops_the_import_in_one_line(beat_tables, settings):
    # Its end, a microsecond earlier, is before the first instant a datetime
    # holds. Under UTC, so no clock change is involved.
    settings.TIME_ZONE = "UTC"
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET expires = %s WHERE id = 1",
            ["0001-01-01 00:00:00"],
        )
    error, printed = import_fails()
    assert str(error) == READ_ERROR
    assert type(error.__cause__) is OverflowError
    assert printed == ""


@pytest.mark.parametrize("column", ["start_time", "expires"])
def test_a_stored_date_read_as_none_stops_the_import(beat_tables, column):
    """
    Django's SQLite converter returns None for date text it cannot parse.
    Read as no bound, a dropped start would run the schedule early and a
    dropped expiry would run it forever, so the import stops instead.
    """
    if connection.vendor != "sqlite":
        pytest.skip("only SQLite keeps date text its converter cannot parse")
    with connection.cursor() as cursor:
        cursor.execute(
            f"UPDATE django_celery_beat_periodictask SET {column} = %s "  # noqa: S608
            "WHERE id = 1",
            ["31/12/2099 10:00"],
        )
    assert decoded_by_driver(column) is None
    error, printed = import_fails()
    assert str(error) == READ_ERROR
    assert printed == ""


def test_a_driver_that_reads_a_stored_date_as_none_stops_the_import(
    beat_tables, monkeypatch
):
    # The same on every backend: a driver that decodes a stored date as None,
    # as mysqlclient does with a MySQL zero date. A NULL bound still imports.
    from django_ox.management.commands.ox_import_beat_schedules import Command

    monkeypatch.setattr(Command, "_parse_datetime", staticmethod(lambda *args: None))
    run()
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET expires = %s WHERE id = 1",
            ["2099-12-31 10:00:00"],
        )
    error, printed = import_fails()
    assert str(error) == READ_ERROR
    assert printed == ""


def test_one_off_task_is_not_translated(beat_tables, recorded):
    # A stored schedule recurs, so a task meant to run once would run again.
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET one_off = %s WHERE id = 1",
            [True],
        )
    output = run()
    assert "nightly" not in calls_by_name(output, recorded)
    assert (
        "#   'nightly': one-off tasks have no equivalent on a stored schedule"
        in output.splitlines()
    )


def test_expired_task_is_not_translated(beat_tables, recorded):
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET expires = %s WHERE id = 1",
            ["2020-01-01 00:00:00"],
        )
    output = run()
    assert "nightly" not in calls_by_name(output, recorded)
    assert "#   'nightly': it has expired" in output.splitlines()


def test_expiry_at_or_before_future_start_is_not_translated(beat_tables, recorded):
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET start_time = %s, "
            "expires = %s WHERE id = 1",
            ["2099-01-02 00:00:00", "2099-01-01 00:00:00"],
        )
    output = run()
    assert "nightly" not in calls_by_name(output, recorded)
    assert (
        "#   'nightly': its expiry is at or before its start time"
        in output.splitlines()
    )


def test_future_start_and_expiry_are_preserved(beat_tables, schedulable):
    future_start = "2099-01-01 10:00:00"
    future_expiry = "2099-12-31 10:00:00"
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET start_time = %s, "
            "expires = %s WHERE id = 1",
            [future_start, future_expiry],
        )

    # On section 2's own printed imports: a call that names datetime is
    # only runnable if the import above it is the right one.
    run_as_module(section_2(run()))

    schedule = OxSchedule.objects.get(name="nightly")
    assert schedule.start_time == instant(future_start)
    # A microsecond short: beat does not run a tick at its expiry, and a
    # stored schedule runs one at its end_time.
    assert schedule.end_time == instant(future_expiry) - timedelta(microseconds=1)


def test_past_start_is_omitted(beat_tables, recorded):
    # create_schedule starts a schedule when it is created, which is later
    # than any start already past, so a past start adds nothing.
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET start_time = %s WHERE id = 1",
            ["2020-01-01 00:00:00"],
        )
    assert "start_time" not in calls_by_name(run(), recorded)["nightly"]


@pytest.mark.parametrize(
    "columns", [(), ("expires",)], ids=["none-of-them", "before-0007"]
)
def test_an_older_table_without_the_later_columns_still_imports(columns, recorded):
    try:
        make_beat_tables(columns)
        if columns:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE django_celery_beat_periodictask SET expires = %s "
                    "WHERE id = 1",
                    ["2099-12-31 10:00:00"],
                )
        calls = calls_by_name(run(), recorded)
    finally:
        drop_beat_tables()
    assert calls["nightly"]["cron"] == "0 2 * * *"
    assert calls["poller"]["every_seconds"] == 5400
    assert ("end_time" in calls["nightly"]) == bool(columns)


def test_disabled_task_remains_disabled(beat_tables, recorded):
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET enabled = %s WHERE id = 1",
            [False],
        )
    calls = calls_by_name(run(), recorded)
    assert calls["nightly"]["enabled"] is False
    assert "enabled" not in calls["poller"]


def test_no_stored_value_can_leave_its_literal(beat_tables, recorded):
    """
    Every stored value reaches the output as data, whatever it holds.

    The output is pasted into settings and into a shell holding production
    credentials. A line break, quote or backslash in a name, a task path, a
    cron field, a zone, a period or an argument must not end its literal or
    its comment line. Each payload sets a marker if it ever runs as code.
    """
    task = 'tasks.a"\n(OX89_TASK := "ran")\n#\\'
    minute = '0" if (OX89_CRON := "ran") else "'
    zone = "UTC\nOX89_ZONE = 'ran'\n#"
    period = "x\u2028OX89_PERIOD = 'ran'\u2028#"
    arguments = {"note": "a\rOX89_ARG = 'ran'\r#", "city": "Z\u00fcrich"}
    names = {
        "cron": "cr\u00f6n\nOX89_NAME = 'ran'\n#",
        "interval": "interval' + (OX89_QUOTE := 'ran') + '\\",
        "one-off": "one-off\u2028OX89_ONE_OFF = 'ran'\u2028#",
        "expired": "expired\rOX89_EXPIRED = 'ran'\r#",
        "order": "order\nOX89_ORDER = 'ran'\n#",
        "orphan": 'orphan" + (OX89_ORPHAN := "ran") + "\n#',
        "zone": "zone\nOX89_ZONE_NAME = 'ran'\n#",
        "period": "period\rOX89_PERIOD_NAME = 'ran'\r#",
    }
    with connection.cursor() as cursor:
        cursor.execute("DELETE FROM django_celery_beat_periodictask")
        cursor.execute(
            "INSERT INTO django_celery_beat_crontabschedule "
            "(id, minute, hour, day_of_month, month_of_year, day_of_week, "
            "timezone) VALUES (2, %s, %s, %s, %s, %s, %s), "
            "(3, %s, %s, %s, %s, %s, %s)",
            [minute, "2", "*", "*", "*", None, "0", "2", "*", "*", "*", zone],
        )
        cursor.execute(
            "INSERT INTO django_celery_beat_intervalschedule (id, every, period) "
            "VALUES (2, %s, %s)",
            [5, period],
        )
    insert_task(1, names["cron"], task, crontab_id=2, kwargs=json.dumps(arguments))
    insert_task(2, names["interval"], task, interval_id=1)
    insert_task(3, names["one-off"], task, crontab_id=1, one_off=True)
    insert_task(4, names["expired"], task, interval_id=1, expires="2020-01-01 00:00:00")
    insert_task(
        5,
        names["order"],
        task,
        interval_id=1,
        start_time="2099-01-02 00:00:00",
        expires="2099-01-01 00:00:00",
    )
    insert_task(6, names["orphan"], task)
    insert_task(7, names["zone"], task, crontab_id=3)
    insert_task(8, names["period"], task, interval_id=2)

    output = run()
    # One physical line for every line the command wrote, whatever a
    # terminal or an editor counts as a line break.
    assert output.isascii()
    assert "\r" not in output

    fragment = {}
    exec("TASKS = {" + section_1(output) + "}", fragment)  # noqa: S102
    assert fragment["TASKS"] == {"SCHEDULABLE_TASKS": {task: task}}
    assert not [key for key in fragment if key.startswith("OX89")]

    for apply in (run_as_module, paste_into_shell):
        recorded.clear()
        namespace = apply(section_2(output))
        assert not [key for key in namespace if key.startswith("OX89")], apply
        assert sorted(recorded, key=lambda call: call["name"]) == [
            {
                "name": names["cron"],
                "task_key": task,
                "trigger": "cron",
                "cron": f"{minute} 2 * * *",
                "arguments": arguments,
            },
            {
                "name": names["interval"],
                "task_key": task,
                "trigger": "interval",
                "every_seconds": 5400,
            },
        ]

    lines = section_2(output).splitlines()
    listed = lines[lines.index("# Not translated, and why:") + 1 :]
    listed = [line for line in listed if line.startswith("#   ")]
    assert {literal_after(line, "#   ") for line in listed} == {
        names[key]
        for key in ("one-off", "expired", "order", "orphan", "zone", "period")
    }
    assert literal_after(output, "its schedule runs in ") == zone
    assert literal_after(output, "an interval of 5 ") == period


def test_one_clock_reading_decides_every_row(beat_tables, clock):
    # Whether a row is translated, why it is not, and the bounds its call
    # carries all come from one instant, so none can contradict another.
    clock("2099-01-01 00:00:00")
    insert_task(4, "expired", "x.y.z", interval_id=1, expires="2098-01-01 00:00:00")
    insert_task(5, "ahead", "x.y.z", interval_id=1, start_time="2099-06-01 00:00:00")
    run()
    assert len(clock.readings) == 1


@pytest.mark.parametrize(
    ("start", "expires", "reason"),
    [
        # Beat counts a task as expired from the instant of its expiry.
        (None, "2099-01-01 00:00:00", "it has expired"),
        (
            "2099-01-02 00:00:00",
            "2099-01-02 00:00:00",
            "its expiry is at or before its start time",
        ),
        (
            "2099-01-02 00:00:00",
            "2099-01-01 23:59:59",
            "its expiry is at or before its start time",
        ),
        # The end_time is a microsecond before the expiry, so here it would
        # equal the start, and create_schedule refuses an end not after it.
        (
            "2099-01-02 00:00:00",
            "2099-01-02 00:00:00.000001",
            (
                "its adjusted end_time is at or before its start time, too short "
                "for a stored schedule"
            ),
        ),
        # With no start printed the schedule starts when it is created,
        # which is no earlier than the import.
        (
            None,
            "2099-01-01 00:00:00.000001",
            "its expiry is one microsecond away, too short for a stored schedule",
        ),
        # A start at the import instant is not ahead of it, so it is left
        # to create_schedule, as if there were none.
        (
            "2099-01-01 00:00:00",
            "2099-01-01 00:00:00.000001",
            "its expiry is one microsecond away, too short for a stored schedule",
        ),
    ],
    ids=[
        "expiry-now",
        "expiry-at-start",
        "expiry-before-start",
        "1us",
        "1us-no-start",
        "1us-start-now",
    ],
)
def test_a_window_no_stored_schedule_can_hold_is_listed(
    beat_tables, recorded, clock, start, expires, reason
):
    clock("2099-01-01 00:00:00")
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET start_time = %s, "
            "expires = %s WHERE id = 1",
            [start, expires],
        )
    output = run()
    assert "nightly" not in calls_by_name(output, recorded)
    assert f"#   'nightly': {reason}" in output.splitlines()


@pytest.mark.parametrize(
    ("start", "expires", "start_time", "end_time"),
    [
        (None, "2099-01-01 00:00:00.000002", None, "2099-01-01 00:00:00.000001"),
        (
            "2099-01-02 00:00:00",
            "2099-01-02 00:00:00.000002",
            "2099-01-02 00:00:00",
            "2099-01-02 00:00:00.000001",
        ),
        # A start at the import instant is not ahead of it, so it is left
        # out and the schedule starts when it is created.
        (
            "2099-01-01 00:00:00",
            "2099-01-03 00:00:00",
            None,
            "2099-01-02 23:59:59.999999",
        ),
    ],
    ids=["2us-no-start", "2us-after-start", "start-now"],
)
def test_the_shortest_window_a_stored_schedule_holds_is_applied(
    beat_tables, schedulable, clock, start, expires, start_time, end_time
):
    now = clock("2099-01-01 00:00:00")
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET start_time = %s, "
            "expires = %s WHERE id = 1",
            [start, expires],
        )
    output = run()
    call = next(line for line in output.splitlines() if "'nightly'" in line)
    assert ("start_time=" in call) == (start_time is not None)
    run_as_module(section_2(output))
    schedule = OxSchedule.objects.get(name="nightly")
    assert schedule.start_time == (instant(start_time) if start_time else now)
    assert schedule.end_time == instant(end_time)


class _ClockChange(tzinfo):
    """
    -05:00, then -04:00 from 2099-03-08 07:00 UTC, when local clocks skip
    from 02:00 to 03:00. Built here rather than read from tz data, whose
    rules for a future year can still change.
    """

    change = datetime(2099, 3, 8, 7)

    def utcoffset(self, dt):
        wall = dt.replace(tzinfo=None, fold=0)
        if wall < datetime(2099, 3, 8, 2):
            return timedelta(hours=-5)
        if wall >= datetime(2099, 3, 8, 3):
            return timedelta(hours=-4)
        # A wall time the change skips, read with the offset from before it
        # unless fold says otherwise, as zoneinfo reads one.
        return timedelta(hours=-4 if dt.fold else -5)

    def dst(self, dt):
        return self.utcoffset(dt) + timedelta(hours=5)

    def tzname(self, dt):
        return None

    def fromutc(self, dt):
        utc = dt.replace(tzinfo=None)
        offset = timedelta(hours=-5 if utc < self.change else -4)
        return (utc + offset).replace(tzinfo=self)


def test_an_expiry_steps_back_on_its_instant_across_a_clock_change():
    # A microsecond before 03:00 on the day clocks skip an hour is 01:59:59
    # and a fraction on the wall. Stepping back on the wall clock gives
    # 02:59:59, a time that never happens, read an hour after the expiry.
    from django_ox.management.commands.ox_import_beat_schedules import Command

    end = Command._end_time(datetime(2099, 3, 8, 3, tzinfo=_ClockChange()))
    assert end.astimezone(UTC) == datetime(2099, 3, 8, 6, 59, 59, 999999, tzinfo=UTC)
    assert end.replace(tzinfo=None) == datetime(2099, 3, 8, 1, 59, 59, 999999)


def test_a_tick_at_the_expiry_does_not_fire(beat_tables, schedulable, clock, settings):
    """
    Beat does not run a tick that falls exactly on its expiry, so the
    imported schedule must not either, although it runs the tick before.
    """
    from django_ox.worker import Worker

    settings.TASKS = {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default"],
            "OPTIONS": {"SCHEDULE_SOURCE": "django_ox.stored.DatabaseScheduleSource"},
        }
    }
    with connection.cursor() as cursor:
        cursor.execute("DELETE FROM django_celery_beat_periodictask WHERE id <> 1")
        # Every minute, so a tick falls exactly on an expiry on the minute.
        cursor.execute(
            "UPDATE django_celery_beat_crontabschedule SET minute = %s, hour = %s",
            ["*", "*"],
        )
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET expires = %s WHERE id = 1",
            ["2099-01-01 02:00:00"],
        )
    clock("2099-01-01 01:00:00")
    run_as_module(section_2(run()))
    key = f"db:{OxSchedule.objects.get(name='nightly').pk}"

    clock("2099-01-01 01:59:00")
    assert Worker(backoff_initial=0).dispatch_schedules() == 1
    expiry = clock("2099-01-01 02:00:00")
    assert Worker(backoff_initial=0).dispatch_schedules() == 0
    assert list(
        OxScheduleTick.objects.filter(schedule_name=key).values_list(
            "scheduled_for", flat=True
        )
    ) == [expiry - timedelta(minutes=1)]


def naive_new_york(settings):
    """
    USE_TZ off in a zone with clock changes, with the worker reading stored
    schedules. The dates are past ones, whose clock changes tz data will not
    move, with the clock pinned before them.
    """
    settings.USE_TZ = False
    settings.TIME_ZONE = "America/New_York"
    settings.TASKS = {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default"],
            "OPTIONS": {"SCHEDULE_SOURCE": "django_ox.stored.DatabaseScheduleSource"},
        }
    }
    with connection.cursor() as cursor:
        cursor.execute("DELETE FROM django_celery_beat_periodictask WHERE id <> 1")
        cursor.execute(
            "UPDATE django_celery_beat_crontabschedule SET minute = %s, hour = %s, "
            "timezone = %s",
            ["*", "*", "America/New_York"],
        )


def test_without_use_tz_an_expiry_after_a_skipped_hour_ends_before_it(
    beat_tables, schedulable, clock, settings
):
    """
    At 03:00 on the day New York skips from 02:00, a microsecond before the
    expiry is 01:59:59 and a fraction. On the wall clock it would be
    02:59:59, which never happens, and PostgreSQL stores that an hour later,
    so the schedule would fire every minute of the hour after its expiry.
    """
    from django_ox.worker import Worker

    naive_new_york(settings)
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET expires = %s WHERE id = 1",
            ["2024-03-10 03:00:00"],
        )
    clock("2024-03-10 00:00:00")
    output = run()
    assert "end_time=datetime.fromisoformat('2024-03-10T01:59:59.999999')" in output
    run_as_module(section_2(output))
    schedule = OxSchedule.objects.get(name="nightly")
    assert schedule.end_time == datetime(2024, 3, 10, 1, 59, 59, 999999)
    fired = {}
    for wall in ("01:59", "03:00", "03:01", "03:59"):
        clock(f"2024-03-10 {wall}:00")
        fired[wall] = Worker(backoff_initial=0).dispatch_schedules()
    assert fired == {"01:59": 1, "03:00": 0, "03:01": 0, "03:59": 0}


@pytest.mark.parametrize(
    ("expires", "end", "fired"),
    [
        # 03:00 happens once, and so does the microsecond before it.
        ("03:00", "02:59:59.999999", {"02:59": 1, "03:00": 0}),
        # 02:00 happens once. The instant one microsecond before it is in
        # the second pass through the repeated hour. The naive stored end
        # is a wall-clock cutoff; these dispatch checks do not distinguish
        # the two passes.
        ("02:00", "01:59:59.999999", {"01:30": 1, "01:59": 1, "02:00": 0}),
    ],
    ids=["after-the-repeat", "end-of-the-repeat"],
)
def test_without_use_tz_an_expiry_after_a_repeated_hour_ends_before_it(
    beat_tables, schedulable, clock, settings, expires, end, fired
):
    """On the day New York repeats 01:00 to 02:00."""
    from django_ox.worker import Worker

    naive_new_york(settings)
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET expires = %s WHERE id = 1",
            [f"2024-11-03 {expires}:00"],
        )
    clock("2024-11-03 00:00:00")
    output = run()
    assert f"end_time=datetime.fromisoformat('2024-11-03T{end}')" in output
    run_as_module(section_2(output))
    assert OxSchedule.objects.get(name="nightly").end_time == datetime.fromisoformat(
        f"2024-11-03 {end}"
    )
    dispatched = {}
    for wall in fired:
        clock(f"2024-11-03 {wall}:00")
        dispatched[wall] = Worker(backoff_initial=0).dispatch_schedules()
    assert dispatched == fired


def test_without_use_tz_a_start_in_a_skipped_hour_can_leave_no_window(
    beat_tables, recorded, clock, settings
):
    """
    02:30 never happens on the day New York skips from 02:00 to 03:00, so an
    expiry at 03:00 steps back to 01:59:59 and a fraction, before that start.
    PostgreSQL moves the skipped start to 03:30 as it stores it, after the
    expiry itself.
    """
    naive_new_york(settings)
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET start_time = %s, "
            "expires = %s WHERE id = 1",
            ["2024-03-10 02:30:00", "2024-03-10 03:00:00"],
        )
    clock("2024-03-01 00:00:00")
    output = run()
    assert "nightly" not in calls_by_name(output, recorded)
    if connection.vendor == "postgresql":
        reason = "its expiry is at or before its start time"
    else:
        reason = (
            "its adjusted end_time is at or before its start time, too short "
            "for a stored schedule"
        )
    assert f"#   'nightly': {reason}" in output.splitlines()


@pytest.mark.parametrize(
    "expires",
    ["2024-11-03 01:00:00", "2024-11-03 01:30:00", "2024-03-10 02:30:00"],
    ids=["repeated-hour-start", "repeated-hour", "skipped-hour"],
)
def test_without_use_tz_an_expiry_that_is_not_one_instant_stops_the_import(
    beat_tables, clock, settings, expires
):
    """
    A naive local time in an hour the clocks repeat names two instants, and
    one in an hour they skip names none, so no end can be derived that
    means what the expiry meant. The import stops rather than guess.
    """
    naive_new_york(settings)
    if connection.vendor == "postgresql" and expires.startswith("2024-03-10"):
        pytest.skip("PostgreSQL moves a skipped local time as it stores it")
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET expires = %s WHERE id = 1",
            [expires],
        )
    clock("2024-03-01 00:00:00")
    error, printed = import_fails()
    assert str(error) == READ_ERROR
    assert printed == ""


@contextmanager
def connection_time_zone(alias, name):
    """Point one connection at another zone, as DATABASES TIME_ZONE would."""
    wrapper = connections[alias]
    original = wrapper.settings_dict["TIME_ZONE"]

    def reset():
        # Read, then delete: the zone is a cached property of the wrapper,
        # and ensure_timezone sets it on the open connection, as Django's
        # own suite does it.
        for attr in ("timezone", "timezone_name"):
            getattr(wrapper, attr)
            delattr(wrapper, attr)
        wrapper.ensure_timezone()

    wrapper.settings_dict["TIME_ZONE"] = name
    reset()
    try:
        yield
    finally:
        wrapper.settings_dict["TIME_ZONE"] = original
        reset()


@pytest.mark.skipif(not settings.USE_TZ, reason="a naive time has no zone")
def test_a_stored_time_is_read_in_the_connection_time_zone(beat_tables, schedulable):
    """
    SQLite and MySQL keep a datetime without its zone, in the connection's
    zone, and PostgreSQL returns one in it. Read in UTC instead, the
    printed times would be five hours off here. A fixed offset: no clock
    change, and nothing that depends on future tz data.
    """
    with connection_time_zone("default", "Etc/GMT+5"):
        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE django_celery_beat_periodictask SET start_time = %s, "
                "expires = %s WHERE id = 1",
                ["2099-01-01 10:00:00", "2099-12-31 10:00:00"],
            )
        output = run()
        # Applied in the same zone, which the stored row is read back in.
        run_as_module(section_2(output))
        schedule = OxSchedule.objects.get(name="nightly")
    call = next(line for line in output.splitlines() if "'nightly'" in line)
    assert "start_time=datetime.fromisoformat('2099-01-01T10:00:00-05:00')" in call
    assert "end_time=datetime.fromisoformat('2099-12-31T09:59:59.999999-05:00')" in call
    assert schedule.start_time == datetime(2099, 1, 1, 15, tzinfo=UTC)
    assert schedule.end_time == datetime(2099, 12, 31, 14, 59, 59, 999999, tzinfo=UTC)


@pytest.mark.skipif(not settings.USE_TZ, reason="a naive time has no zone")
@pytest.mark.django_db(transaction=True, databases=["default", "alt"])
def test_the_zone_comes_from_the_database_it_reads(recorded):
    # --database names the alias holding the beat tables, and its zone is
    # the one their naive values were written in, whatever default's is.
    try:
        make_beat_tables(db="alt")
        with connection_time_zone("alt", "Etc/GMT+5"):
            with connections["alt"].cursor() as cursor:
                cursor.execute(
                    "UPDATE django_celery_beat_periodictask SET start_time = %s "
                    "WHERE id = 1",
                    ["2099-01-01 10:00:00"],
                )
            out = StringIO()
            call_command("ox_import_beat_schedules", database="alt", stdout=out)
    finally:
        drop_beat_tables("alt")
    start = calls_by_name(out.getvalue(), recorded)["nightly"]["start_time"]
    assert start == datetime(2099, 1, 1, 15, tzinfo=UTC)
    assert start.utcoffset() == timedelta(hours=-5)


def test_without_use_tz_a_naive_local_time_is_printed_as_it_was(beat_tables, recorded):
    # Without USE_TZ every datetime is naive local time, and so is the
    # printed one: no offset for create_schedule to convert from.
    with override_settings(USE_TZ=False) if settings.USE_TZ else nullcontext():
        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE django_celery_beat_periodictask SET start_time = %s, "
                "expires = %s WHERE id = 1",
                ["2099-01-01 10:00:00", "2099-12-31 10:00:00"],
            )
        output = run()
    call = next(line for line in output.splitlines() if "'nightly'" in line)
    assert "start_time=datetime.fromisoformat('2099-01-01T10:00:00')" in call
    assert "end_time=datetime.fromisoformat('2099-12-31T09:59:59.999999')" in call
    fields = calls_by_name(output, recorded)["nightly"]
    assert fields["start_time"] == datetime(2099, 1, 1, 10)
    assert fields["end_time"] == datetime(2099, 12, 31, 9, 59, 59, 999999)


def test_without_use_tz_a_stored_offset_is_read_as_local_time(beat_tables, recorded):
    # Only SQLite can hand back an offset here, from text a writer other
    # than Django left. Without USE_TZ the call needs naive local time.
    if connection.vendor != "sqlite":
        pytest.skip("only SQLite returns an offset without USE_TZ")
    with override_settings(USE_TZ=False, TIME_ZONE="Etc/GMT-3"):
        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE django_celery_beat_periodictask SET start_time = %s "
                "WHERE id = 2",
                ["2099-01-01 10:00:00+00:00"],
            )
        output = run()
    start = calls_by_name(output, recorded)["poller"]["start_time"]
    assert start == datetime(2099, 1, 1, 13)
    assert start.tzinfo is None


@pytest.mark.skipif(settings.USE_TZ, reason="the naive settings modules")
def test_without_use_tz_a_naive_local_time_is_stored_as_it_was(
    beat_tables, schedulable
):
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET start_time = %s, "
            "expires = %s WHERE id = 1",
            ["2099-01-01 10:00:00", "2099-12-31 10:00:00"],
        )
    run_as_module(section_2(run()))
    schedule = OxSchedule.objects.get(name="nightly")
    assert schedule.start_time == datetime(2099, 1, 1, 10)
    assert schedule.end_time == datetime(2099, 12, 31, 9, 59, 59, 999999)


@pytest.mark.parametrize(
    ("column", "stored", "reason"),
    [
        ("args", "['emea']", "args contains invalid JSON"),
        ("kwargs", "{'region': 'emea'}", "kwargs contains invalid JSON"),
        # More digits than Python converts from a string by default.
        ("args", "[" + "7" * 5000 + "]", "args contains invalid JSON"),
        (
            "kwargs",
            '{"region": "emea", "n": ' + "7" * 5000 + "}",
            "kwargs contains invalid JSON",
        ),
    ],
    ids=["args-python-repr", "kwargs-python-repr", "args-long-int", "kwargs-long-int"],
)
def test_arguments_that_are_not_json_are_listed_not_dropped(
    beat_tables, recorded, column, stored, reason
):
    # Read as none, they would be translated into a call without them.
    with connection.cursor() as cursor:
        cursor.execute(
            f"UPDATE django_celery_beat_periodictask SET {column} = %s "  # noqa: S608
            "WHERE id = 1",
            [stored],
        )
    output = run()
    assert "nightly" not in calls_by_name(output, recorded)
    assert f"#   'nightly': {reason}" in output.splitlines()


@pytest.mark.parametrize("stored", [None, ""], ids=["null", "empty"])
def test_arguments_stored_as_none_are_still_translated(beat_tables, recorded, stored):
    # Beat reads NULL and an empty string as no arguments, and so does this.
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET args = %s, kwargs = %s "
            "WHERE id = 1",
            [stored, stored],
        )
    calls = calls_by_name(run(), recorded)
    assert "arguments" not in calls["nightly"]


@pytest.mark.parametrize(
    ("column", "stored", "reason"),
    [
        ("kwargs", '{"a": NaN}', "kwargs contains a non-finite number"),
        (
            "kwargs",
            '{"a": [1, {"b": -Infinity}]}',
            "kwargs contains a non-finite number",
        ),
        # Finite in the text, infinite once decoded.
        ("kwargs", '{"a": 1e999}', "kwargs contains a non-finite number"),
        ("args", "[Infinity]", "args contains a non-finite number"),
    ],
    ids=["kwargs-nan", "kwargs-nested", "kwargs-overflow", "args-infinity"],
)
def test_a_non_finite_argument_is_listed_and_later_rows_still_apply(
    beat_tables, recorded, column, stored, reason
):
    """
    Bare nan and inf raise NameError when evaluated. In a module this
    prevents later calls from running; in an interactive shell the
    failing call is not applied. List the unsupported row and check
    that supported rows apply in both modes.
    """
    insert_task(4, "bad", "x.y.z", interval_id=1, **{column: stored})
    insert_task(5, "later", "x.y.z", interval_id=1)
    output = run()
    assert f"#   'bad': {reason}" in output.splitlines()
    for apply in (run_as_module, paste_into_shell):
        recorded.clear()
        apply(section_2(output))
        assert sorted(call["name"] for call in recorded) == [
            "later",
            "nightly",
            "poller",
        ]


DEEP = 900
POSITIONAL = (
    "it passes positional arguments, and a stored schedule takes "
    "keyword arguments only; rewrite the task signature or the row"
)


@pytest.mark.parametrize(
    ("column", "stored", "reason"),
    [
        (
            "args",
            "[" * 600 + "0" + "]" * 600,
            POSITIONAL,
        ),
        (
            "args",
            "[" * DEEP + "0" + "]" * DEEP,
            POSITIONAL,
        ),
        (
            "kwargs",
            '{"a": ' + "[" * DEEP + "Infinity" + "]" * DEEP + "}",
            "kwargs contains a non-finite number",
        ),
    ],
    ids=["args-600", "args-900", "kwargs-900-infinity"],
)
def test_deeply_nested_arguments_are_listed_and_later_rows_still_apply(
    beat_tables, recorded, column, stored, reason
):
    # json.loads decodes lists nested this deep; looking through them for a
    # non-finite number must not run out of recursion and stop the import.
    insert_task(4, "deep", "x.y.z", interval_id=1, **{column: stored})
    insert_task(5, "later", "x.y.z", interval_id=1)
    output = run()
    assert f"#   'deep': {reason}" in output.splitlines()
    assert sorted(calls_by_name(output, recorded)) == ["later", "nightly", "poller"]


@pytest.mark.parametrize(
    ("every", "period"),
    [(float("inf"), "minutes"), (float("-inf"), "seconds"), (1e308, "days")],
    ids=["infinite", "negative-infinite", "infinite-in-seconds"],
)
def test_a_non_finite_interval_is_listed(beat_tables, recorded, every, period):
    # Only SQLite can hold one, as a REAL in the integer column.
    if connection.vendor != "sqlite":
        pytest.skip("only SQLite stores a float in an integer column")
    with connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO django_celery_beat_intervalschedule (id, every, period) "
            "VALUES (2, %s, %s)",
            [every, period],
        )
    insert_task(4, "endless", "x.y.z", interval_id=2)
    insert_task(5, "later", "x.y.z", interval_id=1)
    output = run()
    assert "#   'endless': interval contains a non-finite number" in output.splitlines()
    assert sorted(calls_by_name(output, recorded)) == ["later", "nightly", "poller"]
