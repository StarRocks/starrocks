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
package com.starrocks.sql.automv.util;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * AutoMV resolves base tables through {@code MetadataMgr}, so an external-catalog table reaches
 * {@link MetaUtil#criticalRegion} like any other. Its id is minted by the connector, so locking on it
 * protects nothing -- it either never collides, or collides with an unrelated internal object.
 * <p>
 * The iceberg AutoMV tests only caught this because the connector happened to hand out a negative id for
 * one of the mocked tables; a positive one would have slipped through unnoticed. Hence this direct test.
 */
public class MetaUtilCriticalRegionTest {
    private static final long INTERNAL_DB_ID = 10001L;
    private static final long INTERNAL_TABLE_ID = 10002L;
    /** The shape an Iceberg table id actually had in CI: a hash, negative and meaningless to the FE. */
    private static final long CONNECTOR_MINTED_TABLE_ID = -926727399L;

    private String savedMode;

    @BeforeEach
    public void setUp() {
        GlobalStateMgr.getCurrentState().setLockManager(new LockManager());
        savedMode = Config.lock_target_validation_mode;
        Config.lock_target_validation_mode = "error";
    }

    @AfterEach
    public void tearDown() {
        Config.lock_target_validation_mode = savedMode;
    }

    private static Table tableInCatalog(long id, String catalogName) {
        return new Table(id, "t", Table.TableType.ICEBERG, Lists.newArrayList()) {
            @Override
            public String getCatalogName() {
                return catalogName;
            }
        };
    }

    @Test
    public void testExternalTableRunsTheBlockWithoutALock() {
        Database db = new Database(INTERNAL_DB_ID, "iceberg_db");
        db.setCatalogName("iceberg0");

        AtomicBoolean ran = new AtomicBoolean(false);
        MetaUtil.criticalRegion(db, tableInCatalog(CONNECTOR_MINTED_TABLE_ID, "iceberg0"), LockType.READ, () -> {
            ran.set(true);
            return true;
        });

        Assertions.assertTrue(ran.get());
    }

    @Test
    public void testInternalTableIsStillLocked() {
        Database db = new Database(INTERNAL_DB_ID, "internal_db");

        AtomicBoolean ran = new AtomicBoolean(false);
        MetaUtil.criticalRegion(db, tableInCatalog(INTERNAL_TABLE_ID, "default_catalog"), LockType.READ, () -> {
            ran.set(true);
            return true;
        });

        Assertions.assertTrue(ran.get());
        // The lock was taken and released: a conflicting DB WRITE can be acquired afterwards.
        Locker locker = new Locker();
        locker.lockDatabase(INTERNAL_DB_ID, LockType.WRITE);
        locker.unLockDatabase(INTERNAL_DB_ID, LockType.WRITE);
    }
}
