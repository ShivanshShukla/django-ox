"""
Read a django-celery-beat schedule table and print what django-ox needs.

Writes nothing, anywhere. A migration is a decision about production
timing, so this prints and stops; you read it, edit it, and apply it
yourself.
"""

from __future__ import annotations

import json
from datetime import datetime
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

        rows = self._read(connection)
        if not rows:
            self.stdout.write("No periodic tasks found.")
            return

        paths = sorted({row["task"] for row in rows})
        self.stdout.write(
            "# 1. Expose these tasks. A row can only name a key you list."
        )
        self.stdout.write('"SCHEDULABLE_TASKS": {')
        for path in paths:
            self.stdout.write(f'    "{path}": "{path}",')
        self.stdout.write("},")
        self.stdout.write("")
        self.stdout.write("# 2. Create the schedules.")
        self.stdout.write("from datetime import datetime")
        self.stdout.write("from django_ox.stored import create_schedule")
        self.stdout.write("")

        skipped = []
        for row in rows:
            line = self._as_call(row)
            if line is None:
                skipped.append(row)
                continue
            self.stdout.write(line)

        if skipped:
            self.stdout.write("")
            self.stdout.write("# Not translated, and why:")
            for row in skipped:
                self.stdout.write(f"#   {row['name']}: {self._why(row)}")

        self.stdout.write("")
        self.stdout.write(
            "# Read before applying. Intervals are counted from a fixed instant "
            "here, not\n# from the last run, so their fire times will differ "
            "from Celery's. Schedules\n# preserve their enabled state and start "
            "from the moment you create\n# them unless given a start time. "
            "An expiry near the present can\n# pass before applying, which will "
            "fail validation at creation. A queue\n# or priority set on a beat "
            "task has no equivalent on a stored\n# schedule; set it on the task."
        )

    def _read(self, connection: Any) -> list[dict[str, Any]]:
        tables = connection.introspection.table_names()
        with connection.cursor() as cursor:
            beat_columns = {
                c.name
                for c in connection.introspection.get_table_description(
                    cursor, BEAT_TABLE
                )
            }
            one_off_col = "one_off" if "one_off" in beat_columns else "NULL AS one_off"
            start_time_col = (
                "start_time" if "start_time" in beat_columns else "NULL AS start_time"
            )
            expires_col = "expires" if "expires" in beat_columns else "NULL AS expires"

            cursor.execute(
                f"SELECT name, task, args, kwargs, queue, enabled, "  # noqa: S608
                f"crontab_id, interval_id, {one_off_col}, {start_time_col}, "
                f"{expires_col} FROM {BEAT_TABLE}"
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
            row["_start_time"] = self._parse_datetime(row.get("start_time"), connection)
            row["_expires"] = self._parse_datetime(row.get("expires"), connection)
        return rows

    @staticmethod
    def _parse_datetime(val: Any, connection: Any) -> datetime | None:
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

    def _as_call(self, row: dict[str, Any]) -> str | None:
        if row["_one_off"]:
            return None

        now = timezone.now()
        start_time = row["_start_time"]
        expires = row["_expires"]

        if expires is not None and expires <= now:
            return None
        if (
            start_time is not None
            and start_time > now
            and expires is not None
            and expires <= start_time
        ):
            return None

        name = row["name"]
        if row["_cron"]:
            minute, hour, dom, month, dow, zone = row["_cron"]
            if not self._same_zone(zone):
                return None
            timing = f'trigger="cron", cron="{minute} {hour} {dom} {month} {dow}"'
        elif row["_interval"]:
            every, period = row["_interval"]
            seconds = every * PERIOD_SECONDS.get(period, 0)
            if seconds < 1:
                return None
            timing = f'trigger="interval", every_seconds={seconds}'
        else:
            return None
        if self._positional_args(row):
            return None
        arguments = self._keyword_args(row)

        start_arg = ""
        if start_time is not None and start_time > now:
            start_arg = (
                f", start_time=datetime.fromisoformat({start_time.isoformat()!r})"
            )

        end_arg = ""
        if expires is not None and expires > now:
            end_arg = f", end_time=datetime.fromisoformat({expires.isoformat()!r})"

        enabled = "" if row["enabled"] else ", enabled=False"
        args = f", arguments={arguments!r}" if arguments else ""
        # !r, not a hand-written quoted literal: a name holding a quote or a
        # backslash would otherwise emit code that does not parse, or parses
        # into something else, and the whole output is meant to be pasted.
        return (
            f"create_schedule(name={name!r}, task_key={row['task']!r}, "
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
        try:
            return json.loads(raw) if isinstance(raw, str) else raw
        except (TypeError, ValueError):
            return None

    def _positional_args(self, row: dict[str, Any]) -> bool:
        """A stored schedule passes keyword arguments only."""
        return bool(self._decode(row.get("args")))

    def _keyword_args(self, row: dict[str, Any]) -> dict[str, Any]:
        decoded = self._decode(row.get("kwargs"))
        return decoded if isinstance(decoded, dict) else {}

    def _why(self, row: dict[str, Any]) -> str:
        if row["_one_off"]:
            return "one-off tasks have no equivalent on a stored schedule"
        if row["_cron"] and not self._same_zone(row["_cron"][5]):
            return (
                f"its schedule runs in {row['_cron'][5]}, and a stored "
                f"schedule has no zone of its own: it would run in "
                f"{settings.TIME_ZONE}, at a different time"
            )
        if row["_interval"]:
            every, period = row["_interval"]
            if every * PERIOD_SECONDS.get(period, 0) < 1:
                return (
                    f"an interval of {every} {period} is below one second, "
                    "which the dispatch loop cannot honour"
                )
        if self._positional_args(row):
            return (
                "it passes positional arguments, and a stored schedule takes "
                "keyword arguments only; rewrite the task signature or the row"
            )
        now = timezone.now()
        if row["_expires"] is not None and row["_expires"] <= now:
            return "it has expired"
        if (
            row["_start_time"] is not None
            and row["_start_time"] > now
            and row["_expires"] is not None
            and row["_expires"] <= row["_start_time"]
        ):
            return "its expiry is at or before its start time"
        if row["crontab_id"] or row["interval_id"]:
            return "its schedule row is missing"
        return "solar and clocked schedules have no equivalent"
