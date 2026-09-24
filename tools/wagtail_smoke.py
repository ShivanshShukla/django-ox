"""
Publish a Wagtail page and watch its search-index update run through django-ox.

Wagtail enqueues index updates with ``django_tasks.task`` on every save of an
indexed model, and nothing in the pytest suite drives a real Wagtail project
through that path. This script does, end to end, against a fresh
``wagtail start`` project on SQLite:

1. publish a page whose title carries a unique word, with no task rows before;
2. require queued work and no search hit for the word, because the index
   update has not run yet;
3. start ``manage.py ox_worker --interval 0.2``;
4. poll until no task is READY, RUNNING or WAITING;
5. SIGTERM the worker and require exit 0;
6. require every task SUCCESSFUL and exactly one search hit.

Run it from an environment that has django-ox, Django, Wagtail and
django-tasks installed:

    python tools/wagtail_smoke.py

It exits 0 on success and 1 on failure. Every wait is bounded here rather than
by a shell ``timeout``, so it runs the same way on Linux and macOS. It lives
outside ``tests/`` so pytest never collects it.
"""

import os
import signal
import subprocess
import sys
import tempfile
import time
import traceback
import uuid
from pathlib import Path
from typing import IO

PROJECT = "oxsmoke"
SETTINGS = f"{PROJECT}.settings.dev"

# Generous against a cold CI runner; a healthy run takes a few seconds per step.
SETUP_TIMEOUT = 300.0
DRAIN_TIMEOUT = 120.0
STOP_TIMEOUT = 30.0
POLL_INTERVAL = 0.2

PENDING = ("READY", "RUNNING", "WAITING")

# Appended to the generated settings. The README's configuration, nothing more.
OX_SETTINGS = """

INSTALLED_APPS += ["django_ox"]

TASKS = {
    "default": {
        "BACKEND": "django_ox.backend.OxBackend",
    }
}
"""


class SmokeFailure(Exception):
    pass


def child_env() -> dict[str, str]:
    env = dict(os.environ)
    env["DJANGO_SETTINGS_MODULE"] = SETTINGS
    # The worker log is read back on failure; unbuffered so nothing is lost
    # in a pipe buffer when the process is killed.
    env["PYTHONUNBUFFERED"] = "1"
    return env


def run_setup(argv: list[str], cwd: Path) -> None:
    try:
        done = subprocess.run(  # noqa: S603
            argv,
            cwd=cwd,
            env=child_env(),
            capture_output=True,
            text=True,
            timeout=SETUP_TIMEOUT,
            check=False,
        )
    except subprocess.TimeoutExpired as hung:
        # subprocess.run has already killed it.
        raise SmokeFailure(
            f"{' '.join(argv)} did not finish in {SETUP_TIMEOUT:.0f}s"
        ) from hung
    if done.returncode != 0:
        raise SmokeFailure(
            f"{' '.join(argv)} exited {done.returncode}:\n{done.stdout}{done.stderr}"
        )


def create_project(root: Path) -> None:
    root.mkdir()
    # Through the module rather than the ``wagtail`` script, which is not
    # always installed next to the interpreter.
    run_setup(
        [
            sys.executable,
            "-c",
            "from wagtail.bin.wagtail import main; main()",
            "start",
            PROJECT,
            str(root),
        ],
        cwd=root,
    )
    base = root / PROJECT / "settings" / "base.py"
    base.write_text(base.read_text() + OX_SETTINGS)
    run_setup([sys.executable, "manage.py", "migrate", "--noinput"], cwd=root)


def setup_django(root: Path) -> None:
    sys.path.insert(0, str(root))
    os.environ["DJANGO_SETTINGS_MODULE"] = SETTINGS
    import django

    django.setup()


def task_rows() -> list[tuple[str, str, int]]:
    from django_ox.models import OxTask

    return list(
        OxTask.objects.order_by("enqueued_at").values_list(
            "task_path", "status", "attempts"
        )
    )


def search_hits(word: str) -> int:
    from wagtail.models import Page

    return len(list(Page.objects.live().search(word)))


def publish_page(word: str) -> None:
    from home.models import HomePage
    from wagtail.models import Site

    home = Site.objects.get(is_default_site=True).root_page.specific
    page = home.add_child(
        instance=HomePage(title=f"Smoke {word}", slug=word, live=False)
    )
    page.save_revision().publish()


def start_worker(root: Path, log: IO[bytes]) -> subprocess.Popen[bytes]:
    # The worker opens its own connection; ours must not hold SQLite's lock.
    from django.db import connections

    connections.close_all()
    return subprocess.Popen(
        [sys.executable, "manage.py", "ox_worker", "--interval", "0.2"],
        cwd=root,
        env=child_env(),
        stdout=log,
        stderr=log,
    )


def wait_for_drain(worker: subprocess.Popen[bytes]) -> None:
    from django_ox.models import OxTask

    deadline = time.monotonic() + DRAIN_TIMEOUT
    while True:
        code = worker.poll()
        if code is not None:
            raise SmokeFailure(f"the worker exited {code} before the queue drained")
        if not OxTask.objects.filter(status__in=PENDING).exists():
            return
        if time.monotonic() > deadline:
            raise SmokeFailure(
                f"tasks still pending after {DRAIN_TIMEOUT:.0f}s: "
                "the worker did not drain the queue"
            )
        time.sleep(POLL_INTERVAL)


def stop_worker(worker: subprocess.Popen[bytes]) -> None:
    worker.send_signal(signal.SIGTERM)
    try:
        code = worker.wait(timeout=STOP_TIMEOUT)
    except subprocess.TimeoutExpired as hung:
        raise SmokeFailure(
            f"the worker did not exit within {STOP_TIMEOUT:.0f}s of SIGTERM"
        ) from hung
    if code != 0:
        raise SmokeFailure(f"the worker exited {code} after SIGTERM, expected 0")


def reap(worker: subprocess.Popen[bytes]) -> None:
    """Kill the worker if it is still running, and wait for it either way."""
    if worker.poll() is None:
        worker.kill()
    try:
        worker.wait(timeout=STOP_TIMEOUT)
    except subprocess.TimeoutExpired:
        print(f"worker {worker.pid} survived SIGKILL", file=sys.stderr)


def search_backend_name() -> str:
    from wagtail.search.backends import get_search_backend

    cls = type(get_search_backend())
    return f"{cls.__module__}.{cls.__qualname__}"


def smoke(root: Path, log: IO[bytes]) -> str:
    create_project(root)
    setup_django(root)

    if rows := task_rows():
        raise SmokeFailure(f"expected no task rows before publishing, got {rows}")

    word = f"oxsmoke{uuid.uuid4().hex[:12]}"
    publish_page(word)

    if not task_rows():
        raise SmokeFailure(
            "publishing queued no tasks: the TASKS backend is not django-ox, "
            "or Wagtail ran the index update inline"
        )
    if hits := search_hits(word):
        raise SmokeFailure(
            f"{hits} search hit(s) before the worker ran: the index update did "
            "not wait for the queue"
        )

    worker = start_worker(root, log)
    try:
        wait_for_drain(worker)
        stop_worker(worker)
    finally:
        reap(worker)

    rows = task_rows()
    # LOST and DISCARDED are not pending, so the drain ends on them; they fail here.
    if bad := [row for row in rows if row[1] != "SUCCESSFUL"]:
        raise SmokeFailure(f"{len(bad)} of {len(rows)} task(s) did not end SUCCESSFUL")
    if (hits := search_hits(word)) != 1:
        raise SmokeFailure(f"expected exactly 1 search hit for {word!r}, got {hits}")
    return f"{len(rows)} task(s) SUCCESSFUL, 1 search hit for {word!r}"


def report(failure: Exception, log_path: Path) -> None:
    print(f"wagtail smoke: FAILED: {failure}", file=sys.stderr)
    # Each of these can fail on its own, e.g. before Django is set up.
    try:
        print(f"search backend: {search_backend_name()}", file=sys.stderr)
    except Exception as exc:
        print(f"search backend: unavailable ({exc})", file=sys.stderr)
    try:
        rows = task_rows()
        print(f"task statuses ({len(rows)}):", file=sys.stderr)
        for path, status, attempts in rows:
            print(f"  {status:<10} attempts={attempts} {path}", file=sys.stderr)
    except Exception as exc:
        print(f"task statuses: unavailable ({exc})", file=sys.stderr)
    worker_log = log_path.read_text() if log_path.exists() else ""
    print(f"worker log:\n{worker_log or '  (empty)'}", file=sys.stderr)


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="ox-wagtail-smoke-") as tmp:
        log_path = Path(tmp) / "worker.log"
        with log_path.open("wb") as log:
            try:
                summary = smoke(Path(tmp) / "site", log)
            except Exception as failure:
                if not isinstance(failure, SmokeFailure):
                    traceback.print_exc()
                report(failure, log_path)
                return 1
        print(f"wagtail smoke: ok: {summary}")
        print(f"search backend: {search_backend_name()}")
        return 0


if __name__ == "__main__":
    sys.exit(main())
