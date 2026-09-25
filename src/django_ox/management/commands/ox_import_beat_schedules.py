"""
Read a django-celery-beat schedule table and print what django-ox needs.

Writes nothing, anywhere. A migration is a decision about production
timing, so this prints and stops; you read it, edit it, and apply it
yourself.
"""

from __future__ import annotations

import json
import math
from datetime import UTC, datetime, timedelta, tzinfo
from typing import Any

from django.conf import settings
from django.core.management.base import CommandError
from django.db import DatabaseError, connections, router
from django.utils import timezone

from ...models import OxSchedule
from .._database import DatabaseCommand

# django-celery-beat's tables are read with raw SQL rather than through a
# model, because this package does not depend on it and cannot import one.
# The three names below are module constants, never arguments, so the S608
# suppressions on the queries are about interpolating a constant table name
# and not about user input reaching a statement.
BEAT_TABLE = "django_celery_beat_periodictask"
CRONTAB_TABLE = "django_celery_beat_crontabschedule"
INTERVAL_TABLE = "django_celery_beat_intervalschedule"

FOOTER = (
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
    "# schedule; set it on the task."
)

#: What _decode returns for stored arguments that are not JSON.
_INVALID = object()

PERIOD_SECONDS = {
    "days": 86400,
    "hours": 3600,
    "minutes": 60,
    "seconds": 1,
    "microseconds": 0,
}


class Command(DatabaseCommand):
    help = (
        "Print django-ox equivalents for the schedules in a django-celery-beat table."
    )
    database_help = "Database alias holding the django-celery-beat tables."

    def default_database(self) -> str:
        # db_for_read, not db_for_write: this command reads a table
        # django-ox does not own and writes nothing anywhere. A replica that
        # is behind on somebody else's schedule table still prints the same
        # suggestion, so there is nothing here to pin to the primary.
        return router.db_for_read(OxSchedule)

    def handle(self, *args: Any, **options: Any) -> None:
        alias = self.database(options)
        connection = connections[alias]
        # Reading someone else's table starts by asking which tables are
        # there, so this is where a database that is not there is found.
        # One line, the way the missing-table case below is: this command
        # prints code for a person to read, and a driver traceback in the
        # middle of that is nothing they can act on.
        try:
            tables = connection.introspection.table_names()
        except DatabaseError as exc:
            raise CommandError(f"Database unreachable: {exc}") from exc
        if BEAT_TABLE not in tables:
            raise CommandError(
                f"No {BEAT_TABLE} table on database {alias!r}. Point --database "
                "at the one holding your django-celery-beat schedules."
            )

        # Every row is read and converted before a line is printed. A stored
        # value the driver cannot convert, such as PostgreSQL's 'infinity',
        # an impossible date kept as text on SQLite or text SQLite cannot
        # decode, fails while the cursor is iterated, where no single row can
        # be set aside. A date bound that cannot be carried over as it is
        # stops the import the same way. So it stops, in one line and before
        # any output, rather than printing a traceback or half of what it
        # would have printed.
        try:
            rows = self._read(connection)
        except (DatabaseError, ValueError, OverflowError) as exc:
            raise CommandError(
                f"Cannot read beat schedules from database {alias!a}; check "
                "database access and stored values."
            ) from exc
        if not rows:
            self.stdout.write("No periodic tasks found.")
            return

        # One reading of the clock for the whole import, so whether a row
        # is translated, the reason it is not, and the bounds its call
        # carries are all decided against the same instant.
        now = timezone.now()
        calls = []
        skipped = []
        for row in rows:
            reason = self._skip_reason(row, now)
            if reason is None:
                calls.append(self._as_call(row, now))
            else:
                skipped.append((row["name"], reason))

        paths = sorted({row["task"] for row in rows})
        self.stdout.write(
            "# 1. Expose these tasks. A row can only name a key you list."
        )
        self.stdout.write('"SCHEDULABLE_TASKS": {')
        # Literals through !a, like every stored value below: this fragment
        # is pasted into settings.
        for path in paths:
            self.stdout.write(f"    {path!a}: {path!a},")
        self.stdout.write("},")
        self.stdout.write("")
        self.stdout.write("# 2. Create the schedules.")
        self.stdout.write("from datetime import datetime")
        self.stdout.write("from django_ox.stored import create_schedule")
        self.stdout.write("")

        for call in calls:
            self.stdout.write(call)

        if skipped:
            self.stdout.write("")
            self.stdout.write("# Not translated, and why:")
            for name, reason in skipped:
                # A literal even inside a comment: a line break in a stored
                # name would end the comment and paste the rest as code.
                self.stdout.write(f"#   {name!a}: {reason}")

        self.stdout.write("")
        self.stdout.write(FOOTER)

    def _read(self, connection: Any) -> list[dict[str, Any]]:
        tables = connection.introspection.table_names()
        with connection.cursor() as cursor:
            beat_columns = {
                c.name
                for c in connection.introspection.get_table_description(
                    cursor, BEAT_TABLE
                )
            }
            # expires is in django-celery-beat's first migration, one_off and
            # start_time arrived in its 0007. A table older than that reads
            # them as NULL rather than failing on a column it never had.
            one_off_col = "one_off" if "one_off" in beat_columns else "NULL AS one_off"
            start_time_col = (
                "start_time" if "start_time" in beat_columns else "NULL AS start_time"
            )
            expires_col = "expires" if "expires" in beat_columns else "NULL AS expires"
            # Whether each date bound is NULL, asked of the database rather
            # than read off the decoded value: a driver can decode a stored
            # date it cannot represent as None, as mysqlclient does with
            # MySQL's zero date and Django's SQLite converter with text it
            # cannot parse, and that must not read as a row without a bound.
            nulls = ", ".join(
                f"CASE WHEN {name} IS NULL THEN 1 ELSE 0 END AS {name}_is_null"
                if name in beat_columns
                else f"1 AS {name}_is_null"
                for name in ("start_time", "expires")
            )

            cursor.execute(
                f"SELECT name, task, args, kwargs, queue, enabled, "  # noqa: S608
                f"crontab_id, interval_id, {one_off_col}, {start_time_col}, "
                f"{expires_col}, {nulls} FROM {BEAT_TABLE}"
            )
            columns = [c[0] for c in cursor.description]
            rows = [dict(zip(columns, values, strict=True)) for values in cursor]

            crontabs: dict[Any, Any] = {}
            if CRONTAB_TABLE in tables:
                # django-celery-beat has carried a per-schedule timezone
                # since 2018, but an older table will not have the column
                # and reading it would be a crash rather than a migration.
                crontab_columns = {
                    c.name
                    for c in connection.introspection.get_table_description(
                        cursor, CRONTAB_TABLE
                    )
                }
                zone = "timezone" if "timezone" in crontab_columns else "NULL"
                cursor.execute(
                    f"SELECT id, minute, hour, day_of_month, month_of_year, "  # noqa: S608
                    f"day_of_week, {zone} FROM {CRONTAB_TABLE}"
                )
                crontabs = {r[0]: r[1:] for r in cursor}

            intervals: dict[Any, Any] = {}
            if INTERVAL_TABLE in tables:
                cursor.execute(f"SELECT id, every, period FROM {INTERVAL_TABLE}")  # noqa: S608
                intervals = {r[0]: (r[1], r[2]) for r in cursor}

        for row in rows:
            row["_cron"] = crontabs.get(row["crontab_id"])
            row["_interval"] = intervals.get(row["interval_id"])
            row["_one_off"] = bool(row.get("one_off"))
            row["_start_time"] = self._bound(row, "start_time", connection)
            row["_expires"] = self._bound(row, "expires", connection)
            row["_end_time"] = self._end_time(row["_expires"])
        return rows

    def _bound(
        self, row: dict[str, Any], name: str, connection: Any
    ) -> datetime | None:
        """
        A row's start or expiry, which is None only where the database
        holds NULL. A stored value read as no date at all is not a missing
        bound, and dropping it would run the schedule outside its window.
        """
        value = self._parse_datetime(row[name], connection)
        if value is None and not row[f"{name}_is_null"]:
            raise ValueError(f"{name} is not NULL but was read as no date")
        return value

    @staticmethod
    def _parse_datetime(val: Any, connection: Any) -> datetime | None:
        """
        A stored start or expiry, in the form the rest of the command uses.

        With USE_TZ, Django writes a datetime to SQLite or MySQL without its
        zone, in the zone of the connection: DATABASES TIME_ZONE when set,
        UTC otherwise. So a naive value is read in the zone of the
        connection it came from, the one --database names, not in UTC or in
        TIME_ZONE. PostgreSQL returns aware values, which pass as they are.
        Without USE_TZ every datetime is naive local time, and an aware one
        is brought to it.
        """
        if val is None:
            return None
        if isinstance(val, str):
            val = datetime.fromisoformat(val)
        if isinstance(val, datetime):
            if settings.USE_TZ:
                if timezone.is_naive(val):
                    return timezone.make_aware(val, connection.timezone)
                return val
            if timezone.is_aware(val):
                return timezone.make_naive(val)
            return val
        return None

    @staticmethod
    def _end_time(expires: datetime | None) -> datetime | None:
        """
        The last instant a stored schedule may fire at, for a beat expiry.

        Celery stops at its expiry: a tick that falls exactly on it does not
        run. A stored schedule's end_time still fires a tick that falls on
        it, so the bound moves back by the smallest step a datetime holds.
        The step is taken on the UTC instant rather than on the wall clock,
        where a zone's clock change can skip or repeat an hour: a microsecond
        before 03:00 on a day that skips from 02:00 is 01:59:59 and a
        fraction, while 02:59:59 never happens and PostgreSQL would store it
        an hour later. A naive expiry is local time in TIME_ZONE, so it
        steps back on the instant it names there and is printed as local
        time again. One that happens twice or never there names no single
        instant, and is refused rather than moved.
        """
        if expires is None:
            return None
        step = timedelta(microseconds=1)
        if timezone.is_aware(expires):
            return (expires.astimezone(UTC) - step).astimezone(expires.tzinfo)
        zone = timezone.get_default_timezone()
        if not _happens_once(expires, zone):
            raise ValueError(f"{expires.isoformat()} is not one instant in {zone}")
        return timezone.make_naive(
            timezone.make_aware(expires, zone).astimezone(UTC) - step, zone
        )

    @staticmethod
    def _future_start(row: dict[str, Any], now: datetime) -> datetime | None:
        """
        The row's start time, when it is still ahead.

        create_schedule starts a schedule when it is created, which is later
        than any start already past, so a past start adds nothing.
        """
        start = row["_start_time"]
        return start if start is not None and start > now else None

    def _as_call(self, row: dict[str, Any], now: datetime) -> str:
        """The create_schedule call for a row _skip_reason lets through."""
        if row["_cron"]:
            minute, hour, dom, month, dow, _zone = row["_cron"]
            cron = f"{minute} {hour} {dom} {month} {dow}"
            timing = f'trigger="cron", cron={cron!a}'
        else:
            every, period = row["_interval"]
            seconds = every * PERIOD_SECONDS.get(period, 0)
            timing = f'trigger="interval", every_seconds={seconds}'
        arguments = self._keyword_args(row)

        start_time = self._future_start(row, now)
        start_arg = ""
        if start_time is not None:
            start_arg = (
                f", start_time=datetime.fromisoformat({start_time.isoformat()!a})"
            )

        end_time = row["_end_time"]
        end_arg = ""
        if end_time is not None:
            end_arg = f", end_time=datetime.fromisoformat({end_time.isoformat()!a})"

        enabled = "" if row["enabled"] else ", enabled=False"
        args = f", arguments={arguments!a}" if arguments else ""
        # Escape database-derived text with ascii() and reject non-finite numbers
        # before emitting supported values as Python literals.
        return (
            f"create_schedule(name={row['name']!a}, task_key={row['task']!a}, "
            f"{timing}{args}{start_arg}{end_arg}{enabled})"
        )

    @staticmethod
    def _same_zone(zone: Any) -> bool:
        """
        Does a beat schedule's own zone mean the same as the project's?

        Compared as zones rather than as strings: US/Eastern and
        America/New_York name one zone, and so do Etc/UTC and UTC, so a
        string comparison would divert correctly-aligned rows into the
        untranslated list for no reason.
        """
        if not zone:
            return True
        try:
            from zoneinfo import ZoneInfo

            row_zone = ZoneInfo(str(zone))
            project_zone = ZoneInfo(settings.TIME_ZONE)
        except Exception:
            return str(zone) == settings.TIME_ZONE
        probe = datetime(2026, 1, 1)
        summer = datetime(2026, 7, 1)
        return all(
            probe.replace(tzinfo=row_zone).utcoffset()
            == probe.replace(tzinfo=project_zone).utcoffset()
            for probe in (probe, summer)
        )

    @staticmethod
    def _decode(raw: Any) -> Any:
        """
        Decode stored arguments without validating their JSON shape.

        NULL and an empty string mean no arguments, as they do to beat.
        Caught JSON decoding failures return _INVALID. Valid JSON is passed
        to downstream argument handling; non-object kwargs are currently
        treated as empty kwargs. Stored string args are JSON-decoded and
        non-string values are used unchanged; truthy results trigger the
        positional-arguments reason, while falsy results, including an empty
        object, are treated as no arguments.
        """
        if raw is None or raw == "":
            return None
        if not isinstance(raw, str):
            return raw
        try:
            return json.loads(raw)
        except ValueError:
            return _INVALID

    def _positional_args(self, row: dict[str, Any]) -> bool:
        """A stored schedule passes keyword arguments only."""
        return bool(self._decode(row.get("args")))

    def _keyword_args(self, row: dict[str, Any]) -> dict[str, Any]:
        decoded = self._decode(row.get("kwargs"))
        return decoded if isinstance(decoded, dict) else {}

    def _skip_reason(self, row: dict[str, Any], now: datetime) -> str | None:
        """
        Why a row cannot be translated, or None when it can.

        The one place that decides, so a row is never printed as a call and
        listed as skipped, and the reason listed is the check that failed.
        """
        if row["_one_off"]:
            return "one-off tasks have no equivalent on a stored schedule"
        if row["_cron"]:
            zone = row["_cron"][5]
            if not self._same_zone(zone):
                return (
                    f"its schedule runs in {zone!a}, and a stored "
                    f"schedule has no zone of its own: it would run in "
                    f"{settings.TIME_ZONE!a}, at a different time"
                )
        elif row["_interval"]:
            every, period = row["_interval"]
            seconds = every * PERIOD_SECONDS.get(period, 0)
            if not (math.isfinite(every) and math.isfinite(seconds)):
                return "interval contains a non-finite number"
            if seconds < 1:
                return (
                    f"an interval of {every!a} {period!a} is below one "
                    "second, which the dispatch loop cannot honour"
                )
        elif row["crontab_id"] or row["interval_id"]:
            return "its schedule row is missing"
        else:
            return "solar and clocked schedules have no equivalent"
        for field in ("args", "kwargs"):
            decoded = self._decode(row.get(field))
            if decoded is _INVALID:
                return f"{field} contains invalid JSON"
            if _non_finite(decoded):
                return f"{field} contains a non-finite number"
        if self._positional_args(row):
            return (
                "it passes positional arguments, and a stored schedule takes "
                "keyword arguments only; rewrite the task signature or the row"
            )
        return self._bounds_problem(row, now)

    def _bounds_problem(self, row: dict[str, Any], now: datetime) -> str | None:
        """Why a row's start and expiry leave no window to translate, or None."""
        expires = row["_expires"]
        if expires is None:
            return None
        # Celery counts a task as expired from the instant of its expiry.
        if expires <= now:
            return "it has expired"
        start_time = self._future_start(row, now)
        if start_time is not None and expires <= start_time:
            return "its expiry is at or before its start time"
        # The window the call would carry, not the one beat stored. The end
        # is a microsecond before the expiry, and without a printed start the
        # schedule starts when it is created, after now. create_schedule
        # refuses an end that is not after the start.
        if start_time is not None and row["_end_time"] <= start_time:
            return (
                "its adjusted end_time is at or before its start time, too "
                "short for a stored schedule"
            )
        if start_time is None and row["_end_time"] <= now:
            return "its expiry is one microsecond away, too short for a stored schedule"
        return None


def _happens_once(wall: datetime, zone: tzinfo) -> bool:
    """
    Does a naive local time name exactly one instant in zone?

    Where a clock change skips an hour its times never happen, and where it
    repeats one they happen twice; fold picks between the two readings, and
    only a time that happens once reads the same with either.
    """
    return (
        wall.replace(tzinfo=zone, fold=0).utcoffset()
        == wall.replace(tzinfo=zone, fold=1).utcoffset()
    )


def _non_finite(value: Any) -> bool:
    """
    Does decoded JSON hold NaN or an infinity anywhere? json.loads accepts
    both, and neither has a Python literal to be printed as.
    """
    # Walk with an explicit stack: decoded JSON can be deep enough for
    # a recursive non-finite check to hit Python's recursion limit.
    pending = [value]
    while pending:
        item = pending.pop()
        if isinstance(item, float) and not math.isfinite(item):
            return True
        if isinstance(item, dict):
            pending.extend(item.values())
        elif isinstance(item, list):
            pending.extend(item)
    return False
