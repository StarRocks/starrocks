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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.concurrent.lock.LockHoldDepth;

import java.util.ArrayList;
import java.util.function.Consumer;

/**
 * Samples how many FE metadata locks the creating thread holds in each half of the create contract, and can
 * fail the second half on demand.
 *
 * <p>Sampling the depth rather than asserting on where the call sits in {@link LocalMetastore#onCreate} is
 * deliberate: the property under test is "this work does not happen inside the critical section", which keeps
 * holding if the code around it moves.
 */
public class LockDepthProbeTable extends Table {
    private final boolean failAfterUnlock;

    /** Runs inside the post-unlock hook, for a test that needs something to happen in that window. */
    private Consumer<Database> whileUnlocked;

    /** -1 means the hook was never called. */
    private int depthInOnCreate = -1;
    private int depthInOnCreateAfterUnlock = -1;

    public LockDepthProbeTable(long id, String name, boolean failAfterUnlock) {
        super(id, name, TableType.OLAP, new ArrayList<>());
        this.failAfterUnlock = failAfterUnlock;
    }

    @Override
    public void onCreate(Database database) throws DdlException {
        depthInOnCreate = LockHoldDepth.current();
        super.onCreate(database);
    }

    /**
     * What the probe should do while the database lock is released, before it fails. Lets a test act in
     * exactly the window this hook opened -- a concurrent drop, say -- without any threads.
     */
    public void setWhileUnlocked(Consumer<Database> whileUnlocked) {
        this.whileUnlocked = whileUnlocked;
    }

    @Override
    public void onCreateAfterUnlock(Database database) throws DdlException {
        depthInOnCreateAfterUnlock = LockHoldDepth.current();
        if (whileUnlocked != null) {
            whileUnlocked.accept(database);
        }
        if (failAfterUnlock) {
            throw new DdlException("probe failure after the database lock was released");
        }
    }

    public int getDepthInOnCreate() {
        return depthInOnCreate;
    }

    public int getDepthInOnCreateAfterUnlock() {
        return depthInOnCreateAfterUnlock;
    }
}
