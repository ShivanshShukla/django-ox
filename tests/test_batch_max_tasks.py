"""
`ox_worker --batch` and `--max-tasks`: a worker that finishes without a stop
signal, for cron and job runners (#73).

Every test here runs the real command in a child process, so the exit code,
the drain and the signal handling are the ones a job runner sees.
"""

import json
import signal
import subprocess
import time
from collections import Counter
from datetime import timedelta

import pytest
from django.utils import timezone

from django_ox.models import OxTask

from .conftest import wait_for
from .tasks import echo, enqueue_follow_up, fail_always, slow, swallow_then_run_on
from .test_supervisor import child_env, start_worker

pytestmark = pytest.mark.django_db(transaction=True)


def finish(proc, log, timeout=30):
    """Wait for the worker to exit on its own and return its exit code."""
    try:
        return proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        pytest.fail(f"the worker did not exit on its own:\n{log.read_text()}")
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)


def statuses(results):
    return [OxTask.objects.get(id=r.id).status for r in results]


def counts():
    return Counter(OxTask.objects.values_list("status", flat=True))


def assert_drained_after(log, completion):
    """The completion line, then worker_stopped, which comes after the drain."""
    text = log.read_text()
    assert completion in text, text
    assert "stopped" in text[text.index(completion) :], text


def logged_at(log, text):
    """When the worker logged the first line containing text, as time.time()."""
    for line in log.read_text().splitlines():
        stamp, _, message = line.partition(" ")
        if text in message:
            return float(stamp)
    pytest.fail(f"the worker never logged {text!r}:\n{log.read_text()}")


class TestBatch:
    def test_drains_available_work_then_exits(self, tmp_path):
        results = [echo.enqueue(i) for i in range(3)]
        log = tmp_path / "worker.log"
        proc = start_worker(tmp_path, "--batch", "--interval", "0.05")
        assert finish(proc, log) == 0, log.read_text()
        assert statuses(results) == [OxTask.Status.SUCCESSFUL] * 3
        assert "found nothing to claim after 3 claim(s)" in log.read_text()
        assert "stopped" in log.read_text()

    def test_an_empty_queue_exits_without_waiting_for_the_interval(self, tmp_path):
        env = child_env()
        env["OX_TEST_LOG_FORMAT"] = "%(created)f %(message)s"
        log = tmp_path / "worker.log"
        proc = start_worker(tmp_path, "--batch", "--interval", "5", env=env)
        assert finish(proc, log) == 0, log.read_text()
        exited = time.time()
        # Timed from the worker's own worker_started record. A clock started
        # when the test noticed the line starts late by however long the
        # noticing took, and hides that much of a slow exit.
        assert exited - logged_at(log, "starting") < 2, log.read_text()
        assert "found nothing to claim after 0 claim(s)" in log.read_text()

    def test_leaves_future_work_and_backed_off_retries_ready(self, tmp_path):
        later = echo.using(run_after=timezone.now() + timedelta(hours=1)).enqueue(
            "later"
        )
        failing = fail_always.enqueue()
        log = tmp_path / "worker.log"
        proc = start_worker(tmp_path, "--batch", "--interval", "0.05")
        assert finish(proc, log) == 0, log.read_text()
        assert OxTask.objects.get(id=later.id).status == OxTask.Status.READY
        assert OxTask.objects.get(id=failing.id).attempts == 1

    def test_work_enqueued_by_a_running_task_is_not_left_behind(self, tmp_path):
        first = enqueue_follow_up.enqueue("next")
        log = tmp_path / "worker.log"
        proc = start_worker(
            tmp_path, "--batch", "--concurrency", "4", "--interval", "0.05"
        )
        assert finish(proc, log) == 0, log.read_text()
        assert OxTask.objects.count() == 2
        assert set(OxTask.objects.values_list("status", flat=True)) == {
            OxTask.Status.SUCCESSFUL
        }, log.read_text()
        assert OxTask.objects.get(id=first.id).status == OxTask.Status.SUCCESSFUL

    def test_waits_for_slow_tasks_in_flight(self, tmp_path):
        results = [slow.enqueue(0.5) for _ in range(3)]
        log = tmp_path / "worker.log"
        proc = start_worker(
            tmp_path, "--batch", "--concurrency", "2", "--interval", "0.05"
        )
        assert finish(proc, log) == 0, log.read_text()
        assert statuses(results) == [OxTask.Status.SUCCESSFUL] * 3

    def test_a_signal_during_a_task_still_drains_and_exits_0(self, tmp_path):
        result = slow.enqueue(2)
        log = tmp_path / "worker.log"
        proc = start_worker(tmp_path, "--batch", "--interval", "0.05")
        try:
            assert wait_for(
                lambda: (
                    OxTask.objects.get(id=result.id).status == OxTask.Status.RUNNING
                ),
                timeout=30,
            ), log.read_text()
            proc.send_signal(signal.SIGTERM)
        finally:
            code = finish(proc, log)
        assert code == 0, log.read_text()
        assert OxTask.objects.get(id=result.id).status == OxTask.Status.SUCCESSFUL
        assert "found nothing to claim" not in log.read_text()

    def test_a_task_past_its_timeout_still_recycles_with_75(self, tmp_path):
        env = child_env()
        env["OX_TEST_TASKS_OPTIONS"] = json.dumps(
            {"TASK_TIMEOUT": 0.5, "TASK_TIMEOUT_GRACE": 0.5}
        )
        swallow_then_run_on.enqueue(30, 30)
        log = tmp_path / "worker.log"
        proc = start_worker(tmp_path, "--batch", "--interval", "0.05", env=env)
        assert finish(proc, log, timeout=60) == 75, log.read_text()


class TestMaxTasks:
    def test_never_claims_more_than_the_limit_under_concurrency(self, tmp_path):
        for _ in range(5):
            slow.enqueue(0.3)
        log = tmp_path / "worker.log"
        proc = start_worker(
            tmp_path, "--max-tasks", "2", "--concurrency", "4", "--interval", "0.05"
        )
        assert finish(proc, log) == 0, log.read_text()
        # The limit stops claiming, not running: both claimed attempts ran
        # to the end before the worker exited.
        assert counts() == {
            OxTask.Status.SUCCESSFUL: 2,
            OxTask.Status.READY: 3,
        }, log.read_text()
        assert_drained_after(log, "reached its task limit after 2 claim(s)")

    def test_a_failed_attempt_counts(self, tmp_path):
        failing = fail_always.enqueue()
        never = echo.enqueue("never")
        log = tmp_path / "worker.log"
        proc = start_worker(tmp_path, "--max-tasks", "1", "--interval", "0.05")
        assert finish(proc, log) == 0, log.read_text()
        row = OxTask.objects.get(id=failing.id)
        assert row.attempts == 1
        # The attempt ran and was recorded as a retry, not left RUNNING.
        assert row.status == OxTask.Status.READY, log.read_text()
        assert len(row.errors) == 1
        assert OxTask.objects.get(id=never.id).attempts == 0
        assert "reached its task limit after 1 claim(s)" in log.read_text()

    def test_keeps_polling_an_empty_queue_until_the_limit(self, tmp_path):
        log = tmp_path / "worker.log"
        proc = start_worker(tmp_path, "--max-tasks", "2", "--interval", "0.05")
        try:
            assert wait_for(lambda: "starting" in log.read_text(), timeout=30)
            time.sleep(0.5)
            assert proc.poll() is None, log.read_text()
            echo.enqueue(1)
            echo.enqueue(2)
        finally:
            code = finish(proc, log)
        assert code == 0, log.read_text()
        assert counts() == {OxTask.Status.SUCCESSFUL: 2}, log.read_text()
        assert_drained_after(log, "reached its task limit after 2 claim(s)")


class TestCombined:
    def test_the_limit_first(self, tmp_path):
        for i in range(3):
            echo.enqueue(i)
        log = tmp_path / "worker.log"
        proc = start_worker(
            tmp_path, "--batch", "--max-tasks", "2", "--interval", "0.05"
        )
        assert finish(proc, log) == 0, log.read_text()
        assert counts() == {
            OxTask.Status.SUCCESSFUL: 2,
            OxTask.Status.READY: 1,
        }, log.read_text()
        assert_drained_after(log, "reached its task limit after 2 claim(s)")
        assert "found nothing to claim" not in log.read_text()

    def test_the_empty_queue_first(self, tmp_path):
        echo.enqueue(1)
        log = tmp_path / "worker.log"
        proc = start_worker(
            tmp_path, "--batch", "--max-tasks", "5", "--interval", "0.05"
        )
        assert finish(proc, log) == 0, log.read_text()
        assert "found nothing to claim after 1 claim(s)" in log.read_text()
        assert "reached its task limit" not in log.read_text()


class TestRejected:
    @pytest.mark.parametrize("limit", ["0", "-1", "two", "1.5"])
    def test_an_invalid_limit(self, tmp_path, limit):
        log = tmp_path / "worker.log"
        proc = start_worker(tmp_path, "--max-tasks", limit)
        assert finish(proc, log) == 1, log.read_text()
        assert "--max-tasks must be an integer of at least 1" in log.read_text()
        assert "starting" not in log.read_text()

    @pytest.mark.parametrize("flags", [["--batch"], ["--max-tasks", "3"]])
    def test_with_more_than_one_process(self, tmp_path, flags):
        log = tmp_path / "worker.log"
        proc = start_worker(tmp_path, *flags, "--processes", "2")
        assert finish(proc, log) == 1, log.read_text()
        assert "cannot be combined with --processes" in log.read_text()
        assert "Supervisor" not in log.read_text()
        assert "starting" not in log.read_text()
