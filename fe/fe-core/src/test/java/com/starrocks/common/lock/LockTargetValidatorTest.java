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
package com.starrocks.common.lock;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Database;
import com.starrocks.common.Config;
import com.starrocks.common.util.concurrent.lock.LockInvariantViolations;
import com.starrocks.common.util.concurrent.lock.LockInvariantViolations.Mode;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockParams;
import com.starrocks.common.util.concurrent.lock.LockTargetValidator;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class LockTargetValidatorTest {
    private static final long INTERNAL_DB_ID = 10001L;
    private static final long INTERNAL_TABLE_ID = 10002L;
    /** The default BaseTableInfo#tableId, i.e. "this external base table has no real id". */
    private static final long PLACEHOLDER_TABLE_ID = -1L;

    private String savedMode;
    private long savedLogInterval;

    @BeforeEach
    public void setUp() {
        GlobalStateMgr.getCurrentState().setLockManager(new LockManager());
        savedMode = Config.lock_target_validation_mode;
        savedLogInterval = Config.lock_invariant_violation_log_interval_ms;
        LockInvariantViolations.clearViolations();
    }

    @AfterEach
    public void tearDown() {
        Config.lock_target_validation_mode = savedMode;
        Config.lock_invariant_violation_log_interval_ms = savedLogInterval;
        LockInvariantViolations.clearViolations();
    }

    private static Database internalDb() {
        return new Database(INTERNAL_DB_ID, "internal_db");
    }

    private static Database externalCatalogDb() {
        Database db = new Database(INTERNAL_DB_ID, "hive_db");
        db.setCatalogName("hive_catalog");
        return db;
    }

    // --------------- mode resolution ---------------

    @Test
    public void testModeParsingIsCaseAndWhitespaceInsensitive() {
        Assertions.assertSame(Mode.OFF, Mode.parse("off"));
        Assertions.assertSame(Mode.OFF, Mode.parse(" OFF "));
        Assertions.assertSame(Mode.ERROR, Mode.parse("Error"));
        Assertions.assertSame(Mode.WARN, Mode.parse("warn"));
        // An unrecognized or absent value must not silently disable the check.
        Assertions.assertSame(Mode.WARN, Mode.parse("nonsense"));
        Assertions.assertSame(Mode.WARN, Mode.parse(null));
    }

    @Test
    public void testOffAndErrorPassThroughTheTestEscalation() {
        Assertions.assertSame(Mode.OFF, LockInvariantViolations.effectiveMode("off"));
        Assertions.assertSame(Mode.ERROR, LockInvariantViolations.effectiveMode("error"));
    }

    /**
     * Guards the surefire wiring in fe-core/pom.xml: inside the test JVM {@code warn} has to become
     * {@code error}, otherwise a newly introduced violation would only scroll past in a log.
     */
    @Test
    public void testWarnIsRaisedToErrorInsideTheTestJvm() {
        boolean strict = Boolean.getBoolean("starrocks.lock.invariant.strict.in.test");
        Assertions.assertSame(strict ? Mode.ERROR : Mode.WARN, LockInvariantViolations.effectiveMode("warn"));
    }

    // --------------- external-catalog databases ---------------

    @Test
    public void testLockDatabaseAndCheckExistRefusesExternalCatalogDb() {
        Config.lock_target_validation_mode = "error";
        Locker locker = new Locker();
        IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
                () -> locker.lockDatabaseAndCheckExist(externalCatalogDb(), LockType.READ));
        Assertions.assertTrue(e.getMessage().contains("hive_catalog"), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("internal catalog"), e.getMessage());
    }

    @Test
    public void testLockTableAndCheckDbExistRefusesExternalCatalogDb() {
        Config.lock_target_validation_mode = "error";
        Locker locker = new Locker();
        Assertions.assertThrows(IllegalStateException.class,
                () -> locker.lockTableAndCheckDbExist(externalCatalogDb(), INTERNAL_TABLE_ID, LockType.READ));
    }

    @Test
    public void testInternalDbIsAccepted() {
        Config.lock_target_validation_mode = "error";
        Locker locker = new Locker();
        Database db = internalDb();
        Assertions.assertTrue(locker.lockDatabaseAndCheckExist(db, LockType.READ));
        locker.unLockDatabase(db.getId(), LockType.READ);
        Assertions.assertEquals(0, LockInvariantViolations.totalViolations());
    }

    /**
     * A resource-mapping catalog is not an external catalog: its tables and views are rewritten in
     * place by the FE under exactly these locks, so they must keep them.
     */
    @Test
    public void testResourceMappingCatalogDbIsAccepted() {
        Config.lock_target_validation_mode = "error";
        Database db = new Database(INTERNAL_DB_ID, "hive_db");
        db.setCatalogName("resource_mapping_inside_catalog_hive0");
        Locker locker = new Locker();
        Assertions.assertTrue(locker.lockDatabaseAndCheckExist(db, LockType.READ));
        locker.unLockDatabase(db.getId(), LockType.READ);
        Assertions.assertEquals(0, LockInvariantViolations.totalViolations());
    }

    // --------------- placeholder table ids ---------------

    @Test
    public void testLockTablesWithIntensiveDbLockRefusesPlaceholderId() {
        Config.lock_target_validation_mode = "error";
        Locker locker = new Locker();
        IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
                () -> locker.lockTablesWithIntensiveDbLock(INTERNAL_DB_ID,
                        ImmutableList.of(INTERNAL_TABLE_ID, PLACEHOLDER_TABLE_ID), LockType.READ));
        Assertions.assertTrue(e.getMessage().contains("-1"), e.getMessage());
    }

    @Test
    public void testLockTableWithIntensiveDbLockRefusesZeroId() {
        Config.lock_target_validation_mode = "error";
        Locker locker = new Locker();
        Assertions.assertThrows(IllegalStateException.class,
                () -> locker.lockTableWithIntensiveDbLock(INTERNAL_DB_ID, 0L, LockType.WRITE));
    }

    @Test
    public void testTryLockTablesWithIntensiveDbLockRefusesPlaceholderId() {
        Config.lock_target_validation_mode = "error";
        Locker locker = new Locker();
        Assertions.assertThrows(IllegalStateException.class,
                () -> locker.tryLockTablesWithIntensiveDbLock(INTERNAL_DB_ID,
                        ImmutableList.of(PLACEHOLDER_TABLE_ID), LockType.READ, 10, TimeUnit.MILLISECONDS));
    }

    /**
     * A refusal must happen before anything is acquired. Otherwise the check would trade a bad lock
     * for a stranded one, which is strictly worse.
     */
    @Test
    public void testRefusalStrandsNoLock() {
        Config.lock_target_validation_mode = "error";
        Locker locker = new Locker();
        Assertions.assertThrows(IllegalStateException.class,
                () -> locker.lockTablesWithIntensiveDbLock(INTERNAL_DB_ID,
                        ImmutableList.of(PLACEHOLDER_TABLE_ID), LockType.WRITE));

        // A DB WRITE lock conflicts with the IX the refused call would have taken, so it can only
        // be acquired if that IX was never left behind.
        Locker other = new Locker();
        other.lockDatabase(INTERNAL_DB_ID, LockType.WRITE);
        other.unLockDatabase(INTERNAL_DB_ID, LockType.WRITE);
    }

    // --------------- LockParams ---------------

    @Test
    public void testLockParamsRefusesExternalBaseTable() {
        Config.lock_target_validation_mode = "error";
        LockParams params = new LockParams();
        Assertions.assertThrows(IllegalStateException.class,
                () -> params.add(externalCatalogDb(), INTERNAL_TABLE_ID));
        Assertions.assertThrows(IllegalStateException.class,
                () -> params.add(internalDb(), PLACEHOLDER_TABLE_ID));

        params.add(internalDb(), INTERNAL_TABLE_ID);
        Assertions.assertEquals(1, params.getDbs().size());
        Assertions.assertEquals(1, params.getTables().get(INTERNAL_DB_ID).size());
    }

    // --------------- warn / off ---------------

    /**
     * An observing deployment cannot afford the check to change behaviour: {@code warn} records the
     * violation and returns. Exercised through {@code report} directly because the test JVM raises
     * the configured {@code warn} to {@code error} on purpose.
     */
    @Test
    public void testWarnModeRecordsWithoutRefusing() {
        LockInvariantViolations.report("kind", "detail", "remedy", Mode.WARN);
        Assertions.assertEquals(1, LockInvariantViolations.totalViolations());
    }

    @Test
    public void testOffModeChecksNothing() {
        Config.lock_target_validation_mode = "off";
        Locker locker = new Locker();
        Database db = externalCatalogDb();
        Assertions.assertTrue(locker.lockDatabaseAndCheckExist(db, LockType.READ));
        locker.unLockDatabase(db.getId(), LockType.READ);

        locker.lockTablesWithIntensiveDbLock(INTERNAL_DB_ID, ImmutableList.of(PLACEHOLDER_TABLE_ID), LockType.READ);
        locker.unLockTablesWithIntensiveDbLock(INTERNAL_DB_ID, ImmutableList.of(PLACEHOLDER_TABLE_ID), LockType.READ);

        Assertions.assertEquals(0, LockInvariantViolations.totalViolations());
    }

    // --------------- reporting ---------------

    /**
     * Counts stay exact while logging is throttled: a large interval drops log lines, never counts.
     */
    @Test
    public void testCountsAreExactUnderThrottling() {
        Config.lock_invariant_violation_log_interval_ms = 600_000;
        for (int i = 0; i < 5; i++) {
            LockInvariantViolations.report("kind", "detail", "remedy", Mode.WARN);
        }
        Assertions.assertEquals(5, LockInvariantViolations.totalViolations());
    }

    /**
     * Throttling is per call site, so a site reporting for the first time is never crowded out by a
     * busier one.
     */
    @Test
    public void testThrottlingIsKeyedPerCallSite() {
        Config.lock_invariant_violation_log_interval_ms = 600_000;
        reportFromSiteA();
        reportFromSiteA();
        reportFromSiteB();

        Map<String, Long> bySite = LockInvariantViolations.violationsBySite();
        Assertions.assertEquals(2, bySite.size(), bySite.toString());
        Assertions.assertTrue(bySite.keySet().stream().allMatch(k -> k.contains("LockTargetValidatorTest")),
                bySite.toString());
        Assertions.assertEquals(3, LockInvariantViolations.totalViolations());
    }

    private void reportFromSiteA() {
        LockInvariantViolations.report("kind", "detail", "remedy", Mode.WARN);
    }

    private void reportFromSiteB() {
        LockInvariantViolations.report("kind", "detail", "remedy", Mode.WARN);
    }

    @Test
    public void testReportIsANoOpWhenOff() {
        LockInvariantViolations.report("kind", "detail", "remedy", Mode.OFF);
        Assertions.assertEquals(0, LockInvariantViolations.totalViolations());
    }

    // --------------- report format: the log has to stay greppable ---------------

    /**
     * fe.warn.log is busy, so a violation needs a stable machine anchor rather than a sentence.
     * The same format is used for the exception, so one grep covers a running FE and a CI failure.
     */
    @Test
    public void testReportCarriesTheGrepTagAndFields() {
        Config.lock_target_validation_mode = "error";
        Locker locker = new Locker();
        IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
                () -> locker.lockDatabaseAndCheckExist(externalCatalogDb(), LockType.READ));

        String message = e.getMessage();
        Assertions.assertTrue(message.startsWith(LockInvariantViolations.LOG_TAG + " "), message);
        Assertions.assertTrue(message.contains("kind=" + "external_catalog_database"), message);
        // `grep -o 'kind=[^ ]*'` and `site=[^ ]*` only work while these values hold no whitespace.
        assertFieldValueHasNoSpace(message, "kind=");
        assertFieldValueHasNoSpace(message, "site=");
    }

    @Test
    public void testPlaceholderIdReportUsesItsOwnKind() {
        Config.lock_target_validation_mode = "error";
        Locker locker = new Locker();
        IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
                () -> locker.lockTableWithIntensiveDbLock(INTERNAL_DB_ID, 0L, LockType.WRITE));
        Assertions.assertTrue(e.getMessage().contains("kind=" + "placeholder_table_id"), e.getMessage());
    }

    /**
     * A newline or a quote reaching the report -- via a database name, say -- would split one
     * violation across lines and break every line-oriented grep above.
     */
    @Test
    public void testReportStaysOnOneLine() {
        IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
                () -> LockInvariantViolations.report("kind", "a\nb\"c", "remedy", Mode.ERROR));
        Assertions.assertEquals(1, e.getMessage().lines().count(), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("detail=\"a b'c\""), e.getMessage());
    }

    private static void assertFieldValueHasNoSpace(String message, String field) {
        int start = message.indexOf(field);
        Assertions.assertTrue(start >= 0, field + " missing from: " + message);
        String value = message.substring(start + field.length()).split(" ", 2)[0];
        Assertions.assertFalse(value.isEmpty(), field + " has an empty value in: " + message);
    }

    // --------------- the check must never be what breaks locking ---------------

    /**
     * A half-constructed or otherwise hostile {@link Database} must cost the caller nothing: the
     * verdict is computed inside a catch-all. The report has to stay outside it, or the refusal
     * ERROR mode raises would be swallowed by that same catch -- which is why
     * {@link #testLockDatabaseAndCheckExistRefusesExternalCatalogDb} exists next to this one.
     */
    @Test
    public void testInspectionFailureDoesNotBreakLocking() {
        Config.lock_target_validation_mode = "error";
        Database hostile = new Database(INTERNAL_DB_ID, "boom") {
            @Override
            public String getCatalogName() {
                throw new IllegalArgumentException("metastore is half-built");
            }
        };
        Locker locker = new Locker();
        Assertions.assertTrue(locker.lockDatabaseAndCheckExist(hostile, LockType.READ));
        locker.unLockDatabase(hostile.getId(), LockType.READ);
        Assertions.assertEquals(0, LockInvariantViolations.totalViolations());
    }

    // --------------- direct validator entry points ---------------

    @Test
    public void testNullInputsAreHandled() {
        Config.lock_target_validation_mode = "error";
        LockTargetValidator.validateDatabase(null);
        LockTargetValidator.validateTableIds(null);
        Assertions.assertThrows(IllegalStateException.class, () -> LockTargetValidator.validateTableId(null));
    }
}
