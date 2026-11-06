# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Waiting for a StarRocks schema-change job to end, shared by every caller that waits for one.

StarRocks runs a column change, an index build or a distribution change on a table as an asynchronous job, listed
by ``SHOW ALTER TABLE COLUMN`` or ``OPTIMIZE``, and refuses another change to the table while the job runs. Each
caller reads the table's newest job in its own way, and decides what a cancelled job, or one still running at its
deadline, means for it. The polling in between is :func:`wait_for_job_to_end`.
"""

from enum import Enum
import logging
import time
from typing import Any, Callable, Mapping, NamedTuple, Optional


logger = logging.getLogger(__name__)


class SchemaChangeState(str, Enum):
    """Job states reported by the ``State`` column of ``SHOW ALTER TABLE COLUMN``."""

    PENDING = "PENDING"
    WAITING_TXN = "WAITING_TXN"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    CANCELLED = "CANCELLED"

    @classmethod
    def parse(cls, value: Any) -> Optional["SchemaChangeState"]:
        """Return the matching member, or ``None`` for a state we don't know.

        An unknown state is treated as "still in progress" by the caller, so a
        state added by a future StarRocks version keeps the migration waiting
        instead of failing it.
        """
        try:
            return cls(str(value).upper())
        except ValueError:
            return None


class SchemaChangeJob(NamedTuple):
    """A StarRocks schema-change job, as a row of ``SHOW ALTER TABLE COLUMN`` or ``OPTIMIZE`` gives it."""

    job_id: str
    state: str
    message: str

    @classmethod
    def from_row(cls, row: Optional[Mapping[str, Any]]) -> Optional["SchemaChangeJob"]:
        """Return the job a ``SHOW ALTER TABLE`` row describes, or None for no row or a row without a State."""
        if row is None or row.get("State") is None:
            return None
        return cls(str(row.get("JobId", "?")), str(row["State"]), str(row.get("Msg") or ""))

    @property
    def has_ended(self) -> bool:
        """Whether the job is FINISHED or CANCELLED. A state that StarRocks adds later counts as still running."""
        return SchemaChangeState.parse(self.state) in (SchemaChangeState.FINISHED, SchemaChangeState.CANCELLED)

    @property
    def was_cancelled(self) -> bool:
        """Whether StarRocks cancelled the job."""
        return SchemaChangeState.parse(self.state) is SchemaChangeState.CANCELLED


def wait_for_job_to_end(
    read_newest_job: Callable[[], Optional[SchemaChangeJob]],
    kind: str,
    table: str,
    poll_seconds: float,
    deadline: Optional[float],
) -> Optional[SchemaChangeJob]:
    """Poll the table's newest job of a kind until it has ended, and return the job the wait stopped on.

    That is None when ``read_newest_job`` finds no job, the job once it has ended, FINISHED or CANCELLED, or the
    job still running when the ``time.monotonic()`` deadline passes; a deadline of None waits without a limit. The
    caller decides what a cancelled or a still-running job means. ``kind`` and ``table`` name the job in the log,
    where each poll is written at INFO.
    """
    polls = 0
    job = read_newest_job()
    while job is not None and not job.has_ended:
        if deadline is not None and time.monotonic() >= deadline:
            return job
        polls += 1
        logger.info(
            "Waiting for the %s job %s on table %r to end (state: %s, polls: %d).",
            kind, job.job_id, table, job.state, polls,
        )
        time.sleep(_seconds_until_next_poll(poll_seconds, deadline))
        job = read_newest_job()
    return job


def _seconds_until_next_poll(poll_seconds: float, deadline: Optional[float]) -> float:
    """Return the poll interval, or less when the deadline comes sooner."""
    if deadline is None:
        return poll_seconds
    return min(poll_seconds, max(deadline - time.monotonic(), 0.0))
