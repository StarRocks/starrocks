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

package com.starrocks.server;

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.common.Config;
import com.starrocks.common.util.FrontendDaemon;

/**
 * Runs the expired-job cleaner (load, export, delete, routine load, stream load, transactions, backup jobs
 * and task runs) once every {@code label_clean_interval_second}.
 * <p>
 * The configured interval is typically hours long, so instead of sleeping for the whole interval the daemon
 * wakes up every {@link #TICK_MS} and re-reads the mutable config. Changing the interval at runtime therefore
 * takes effect within a minute rather than after the previous multi-hour sleep completes.
 */
public class LoadLabelCleaner extends FrontendDaemon {
    @VisibleForTesting
    static final long TICK_MS = 60_000L;

    private final Runnable cleaner;
    // Zero so that the first tick cleans immediately, matching the previous behavior of cleaning once at startup.
    private long lastCleanMs = 0L;

    public LoadLabelCleaner(Runnable cleaner) {
        super("LoadLabelCleaner", TICK_MS);
        this.cleaner = cleaner;
    }

    @Override
    protected void runAfterCatalogReady() {
        tick(System.currentTimeMillis());
    }

    @VisibleForTesting
    void tick(long nowMs) {
        if (nowMs - lastCleanMs >= Config.label_clean_interval_second * 1000L) {
            cleaner.run();
            lastCleanMs = nowMs;
        }
    }
}
