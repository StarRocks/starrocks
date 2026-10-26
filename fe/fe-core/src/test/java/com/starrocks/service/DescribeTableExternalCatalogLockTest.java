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

package com.starrocks.service;

import com.starrocks.catalog.Database;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.hive.MockedHiveMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.thrift.TDescribeTableParams;
import com.starrocks.thrift.TDescribeTableResult;
import com.starrocks.thrift.TUserIdentity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@code FrontendServiceImpl.describeTable} resolves the database and table through {@code MetadataMgr}, so
 * for an external catalog both ids it then locks are minted by the connector. The two id spaces are not
 * partitioned, so on a collision a DESC against Hive takes IS + READ on an unrelated internal table and stalls
 * DDL there.
 * <p>
 * The lock-target validator cannot catch this one: {@code ConnectorTableId} offsets by 100,000,000, so a
 * connector id is always a large positive number and the placeholder check never fires. The call site has to
 * decline the lock itself, which is what this test pins down.
 */
public class DescribeTableExternalCatalogLockTest extends ConnectorPlanTestBase {

    private static final long COLLIDING_DB_ID = 987654321L;

    /** Mirrors MockedHiveMetadata except that the database id is fixed, so the test can contend on it. */
    private static class FixedIdHiveMetadata extends MockedHiveMetadata {
        @Override
        public Database getDb(ConnectContext context, String dbName) {
            return new Database(COLLIDING_DB_ID, dbName);
        }
    }

    @AfterEach
    public void restoreHiveMetadata() {
        MockedMetadataMgr metadataMgr = (MockedMetadataMgr) GlobalStateMgr.getCurrentState().getMetadataMgr();
        metadataMgr.registerMockedMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME, new MockedHiveMetadata());
    }

    private static void useFixedIdHiveMetadata() {
        MockedMetadataMgr metadataMgr = (MockedMetadataMgr) GlobalStateMgr.getCurrentState().getMetadataMgr();
        metadataMgr.registerMockedMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME, new FixedIdHiveMetadata());
    }

    private static TDescribeTableParams describeParams(String catalogName, String dbName, String tableName) {
        TDescribeTableParams params = new TDescribeTableParams();
        params.setCatalog_name(catalogName);
        params.setDb(dbName);
        params.setTable_name(tableName);
        TUserIdentity user = new TUserIdentity();
        user.setUsername("root");
        user.setHost("%");
        user.setIs_domain(false);
        params.setCurrent_user_ident(user);
        return params;
    }

    /**
     * Runs a DESC on another thread while this thread holds a WRITE lock on {@code contendedDbId}.
     *
     * @return true if it completed while the lock was held, i.e. it never tried to lock that id.
     */
    private static boolean completesWhileDbIsWriteLocked(TDescribeTableParams params, long contendedDbId)
            throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicReference<TDescribeTableResult> result = new AtomicReference<>();

        Locker holder = new Locker();
        holder.lockDatabase(contendedDbId, LockType.WRITE);
        Thread worker = new Thread(() -> {
            try {
                result.set(new FrontendServiceImpl(null).describeTable(params));
            } catch (Throwable t) {
                error.set(t);
            } finally {
                done.countDown();
            }
        });
        try {
            worker.start();
            boolean completed = done.await(3, TimeUnit.SECONDS);
            if (completed && error.get() != null) {
                throw new RuntimeException(error.get());
            }
            return completed;
        } finally {
            holder.unLockDatabase(contendedDbId, LockType.WRITE);
            // If it was blocked, releasing lets it finish; never leave the thread parked.
            worker.join(TimeUnit.SECONDS.toMillis(20));
            if (error.get() != null) {
                throw new RuntimeException(error.get());
            }
        }
    }

    /**
     * Control: the same harness pointed at a genuine internal table must observe the lock. Without it, the
     * external case below would pass even if the detector were blind, or if the fix had simply removed the
     * lock for everyone.
     */
    @Test
    public void testInternalTableStillLocks() throws Exception {
        long internalDbId = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test").getId();
        Assertions.assertFalse(
                completesWhileDbIsWriteLocked(describeParams(null, "test", "t0"), internalDbId),
                "DESC on an internal table must still block behind a DB WRITE lock");
    }

    @Test
    public void testExternalCatalogTableTakesNoLock() throws Exception {
        useFixedIdHiveMetadata();
        Assertions.assertTrue(
                completesWhileDbIsWriteLocked(
                        describeParams(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME, "tpch", "customer"),
                        COLLIDING_DB_ID),
                "DESC on an external catalog must not lock the connector-minted database id");
    }

    @Test
    public void testDescribeStillReturnsColumnsForBothCatalogKinds() throws Exception {
        Assertions.assertFalse(
                new FrontendServiceImpl(null).describeTable(describeParams(null, "test", "t0"))
                        .getColumns().isEmpty());
        Assertions.assertFalse(
                new FrontendServiceImpl(null)
                        .describeTable(describeParams(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME,
                                "tpch", "customer"))
                        .getColumns().isEmpty());
    }
}
