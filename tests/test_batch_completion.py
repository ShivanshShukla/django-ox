"""
How a `--batch` or `--max-tasks` run ends, seen from inside the worker.

test_batch_max_tasks.py runs the real command, which is what a job runner
sees, but only from outside: the log it reads is message text, and the
moments inside a pass are out of its reach. Here the worker runs on a thread
of the test's own, so a test can hold a task at a chosen point in a pass and
read each log record whole, with the event name, level and keys that
monitoring depends on.
"""

import logging
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait as wait_futures

import pytest

import django_ox.worker
from django_ox.models import OxTask
from django_ox.worker import Worker

from . import tasks
from .conftest import start_worker_thread
from .tasks import echo, enqueue_follow_up_once_released, fail_always

pytestmark = pytest.mark.django_db(transaction=True)

COMPLETIONS = ("worker_batch_empty", "worker_max_tasks_reached")


def lifecycle(caplog):
    """The completion and stop records, in the order they were logged."""
    return [
        record
        for record in caplog.records
        if getattr(record, "event", None) in (*COMPLETIONS, "worker_stopped")
    ]


def run_to_completion(worker, caplog):
    with caplog.at_level(logging.INFO, logger="django_ox"):
        thread = start_worker_thread(worker)
        thread.join(timeout=10)
    assert not thread.is_alive(), "the worker never finished on its own"


def counts():
    return Counter(OxTask.objects.values_list("status", flat=True))


class TestIdleIsReadBeforeTheClaim:
    def test_a_follow_up_enqueued_during_an_empty_claim_still_runs(
        self, monkeypatch, caplog
    ):
        """
        A task running when a pass begins keeps that pass from ending the
        batch, even when it enqueues a follow-up and finishes while the
        claim is finding nothing. Read after the claim instead, the worker
        would look idle and the queue empty, and the batch would end with
        the follow-up READY. The claim below holds the pass at exactly that
        point: the task is let go only once a pass that began with it in
        flight has found nothing, and that claim returns only once run()
        would see the task finished.
        """
        release = tasks.STATE["release"] = threading.Event()
        submitted = []

        class RecordingPool(ThreadPoolExecutor):
            def submit(self, fn, /, *args, **kwargs):
                future = super().submit(fn, *args, **kwargs)
                submitted.append(future)
                return future

        # run() judges a task finished by its future, which completes a
        # moment after the task function returns, so the test waits on the
        # future itself. The pool is local to run(); this is how to reach it.
        monkeypatch.setattr(django_ox.worker, "ThreadPoolExecutor", RecordingPool)
        enqueue_follow_up_once_released.enqueue("follow-up")
        worker = Worker(poll_interval=0.02, concurrency=2, batch=True)
        real_claim = worker.claim_one
        in_flight_at_empty_claim = []
        finished_before_return = []

        def claim():
            found = real_claim()
            if found is None:
                in_flight_at_empty_claim.append(not submitted[0].done())
                # With a free slot, the first pass claims the task and then
                # finds nothing in the same pass. The second pass is the
                # first to begin with the task in flight.
                if len(in_flight_at_empty_claim) == 2:
                    release.set()
                    wait_futures(submitted[:1], timeout=10)
                    finished_before_return.append(submitted[0].done())
            return found

        monkeypatch.setattr(worker, "claim_one", claim)
        run_to_completion(worker, caplog)

        assert in_flight_at_empty_claim[:2] == [True, True]
        assert finished_before_return == [True]
        assert counts() == {OxTask.Status.SUCCESSFUL: 2}, (
            "the batch ended with the follow-up still due"
        )
        (done, _) = lifecycle(caplog)
        assert done.event == "worker_batch_empty"
        assert done.claimed == 2


class TestTheCompletionEvent:
    @pytest.mark.parametrize(
        ("options", "event", "claimed", "after"),
        [
            (
                {"batch": True},
                "worker_batch_empty",
                3,
                {OxTask.Status.SUCCESSFUL: 3},
            ),
            (
                {"max_tasks": 2},
                "worker_max_tasks_reached",
                2,
                {OxTask.Status.SUCCESSFUL: 2, OxTask.Status.READY: 1},
            ),
        ],
        ids=["batch", "max-tasks"],
    )
    def test_one_info_record_then_worker_stopped(
        self, options, event, claimed, after, caplog
    ):
        # The name, the level and the keys are the contract an operator
        # alerts on; the message text is not.
        for i in range(3):
            echo.enqueue(i)
        worker = Worker(poll_interval=0.02, concurrency=4, **options)
        run_to_completion(worker, caplog)

        records = lifecycle(caplog)
        assert [record.event for record in records] == [event, "worker_stopped"]
        done = records[0]
        assert done.levelno == logging.INFO
        assert done.worker_id == worker.worker_id
        assert done.claimed == claimed
        # The run ends by draining: every attempt it claimed ran first.
        assert counts() == after

    @pytest.mark.parametrize(
        ("options", "queued"),
        [({"batch": True}, 0), ({"max_tasks": 1}, 1)],
        ids=["batch", "max-tasks"],
    )
    def test_a_stop_already_requested_logs_neither(
        self, options, queued, monkeypatch, caplog
    ):
        """
        The stop lands inside the pass that would have completed the run,
        after the claim loop last checked for one. The run ended because
        something asked it to, and the log must not say it completed.
        """
        results = [echo.enqueue(i) for i in range(queued)]
        worker = Worker(poll_interval=0.02, **options)
        real_claim = worker.claim_one

        def claim_then_stop():
            found = real_claim()
            worker.request_stop()
            return found

        monkeypatch.setattr(worker, "claim_one", claim_then_stop)
        run_to_completion(worker, caplog)

        assert [record.event for record in lifecycle(caplog)] == ["worker_stopped"]
        assert [OxTask.objects.get(id=r.id).status for r in results] == [
            OxTask.Status.SUCCESSFUL
        ] * queued


class TestTheTaskLimitCountsAttempts:
    def test_a_retry_claimed_again_counts(self, caplog):
        """
        The limit is on attempts, not tasks: a failed task whose retry is
        due at once is claimed again, and that claim spends one more.
        """
        failing = fail_always.enqueue()
        worker = Worker(backoff_initial=0, poll_interval=0.02, max_tasks=2)
        run_to_completion(worker, caplog)

        row = OxTask.objects.get(id=failing.id)
        assert row.attempts == 2
        # Retried rather than failed for good: the limit stopped a third.
        assert row.status == OxTask.Status.READY
        (done, _) = lifecycle(caplog)
        assert done.event == "worker_max_tasks_reached"
        assert done.claimed == 2
