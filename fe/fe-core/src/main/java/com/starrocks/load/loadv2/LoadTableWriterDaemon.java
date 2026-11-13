// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.load.loadv2;

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.load.loadv2.LoadsHistoryWriter.FlushResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.function.BooleanSupplier;

/**
 * Leader daemon that drains one {@link LoadsHistoryWriter} into its table.
 *
 * <p>Each cycle flushes a batch and then runs the table-specific work of the subclass
 * (the loads_history backstop, the load_jobs reconciliation). Rows are accepted as soon as
 * this FE leads; they wait in the writer until the table keeper reports the schema ready,
 * since the explicit column lists would not resolve before that. Whatever is pending when
 * this FE stops being leader is discarded along with the subclass's session state.
 */
abstract class LoadTableWriterDaemon extends LeaderDaemon {
    private static final Logger LOG = LogManager.getLogger(LoadTableWriterDaemon.class);

    protected final LoadsHistoryWriter writer;
    private final BooleanSupplier schemaReady;
    private boolean schemaNotReadyLogged = false;

    LoadTableWriterDaemon(String name, LoadsHistoryWriter writer, BooleanSupplier schemaReady) {
        // No inter-cycle delay: each cycle paces itself on the writer's bounded wait for a batch.
        super(name, 0L);
        this.writer = writer;
        this.schemaReady = schemaReady;
    }

    @VisibleForTesting
    LoadsHistoryWriter getWriter() {
        return writer;
    }

    /** Make {@link #writer} the one that accepts rows on this node. */
    protected abstract void activateWriter();

    protected abstract void deactivateWriter();

    /** The table-specific work of one cycle, after the flush that returned {@code result}. */
    protected abstract void afterFlush(FlushResult result);

    /** Forget the state of the leader session that just ended. */
    protected abstract void resetSession();

    @Override
    protected void runAfterLeaseValid() throws InterruptedException {
        long flushIntervalMs = Math.max(1, Config.loads_history_flush_interval_second) * 1000L;
        if (FeConstants.runningUnitTest) {
            // Unit tests have no executor to write to.
            Thread.sleep(flushIntervalMs);
            return;
        }
        activateWriter();
        if (!schemaReady.getAsBoolean()) {
            if (!schemaNotReadyLogged) {
                LOG.info("{} is missing or lacks columns, waiting for the table keeper", writer.getTarget().tableName());
                schemaNotReadyLogged = true;
            }
            Thread.sleep(flushIntervalMs);
            return;
        }
        schemaNotReadyLogged = false;
        try {
            runCycle(flushIntervalMs);
        } catch (InterruptedException e) {
            throw e;
        } catch (Throwable e) {
            LOG.warn("Failed to process one round of {} with error message {}", getName(), e.getMessage(), e);
        }
    }

    /** One cycle: flush pending rows, then the table-specific work. */
    @VisibleForTesting
    void runCycle(long flushIntervalMs) throws InterruptedException {
        FlushResult result = writer.runOnce(Config.loads_history_flush_batch_rows, flushIntervalMs);
        afterFlush(result);
    }

    @Override
    protected void onStopped() {
        deactivateWriter();
        writer.clear();
        resetSession();
        schemaNotReadyLogged = false;
    }
}
