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

package com.starrocks.common.util.concurrent.lock;

import com.starrocks.catalog.Database;
import com.starrocks.common.util.concurrent.lock.LockInvariantViolations.Mode;
import com.starrocks.server.CatalogMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;

/**
 * Enforces the one rule the {@link LockManager} key space cannot express:
 *
 * <blockquote>only an object owned by the internal catalog may be a metadata lock target</blockquote>
 *
 * {@code LockManager} is keyed by a bare {@code long rid}. A long says nothing about what it
 * identifies or where it came from, which is exactly why three call sites could lock identities a
 * connector had invented and look entirely normal doing it:
 *
 * <table>
 *     <caption>locks that were never real</caption>
 *     <tr><th>what was locked</th><th>what actually happened</th></tr>
 *     <tr><td>a Hive database, freshly minted from {@code CONNECTOR_ID_GENERATOR} on every
 *             resolve</td>
 *         <td>a lock nobody else can ever name — a no-op</td></tr>
 *     <tr><td>a JDBC database, whose id is always {@code 0}</td>
 *         <td>every JDBC database serialized onto one lock</td></tr>
 *     <tr><td>an external base table, whose {@code BaseTableInfo.tableId} stays at its default
 *             {@code -1}</td>
 *         <td>every external base table in the FE serialized onto one lock</td></tr>
 * </table>
 *
 * <h3>What is checked, and what deliberately is not</h3>
 *
 * Only what the caller already holds, and never the catalog:
 * <ul>
 *     <li>the {@link Database} handed in belongs to an external catalog — one field read and a
 *         string comparison;</li>
 *     <li>the table id is a placeholder ({@code -1} or {@code 0}) — one comparison, no lookup and
 *         no allocation.</li>
 * </ul>
 *
 * Neither can be turned into a false positive by concurrent DDL, which is what makes them safe to
 * refuse outright rather than merely warn about.
 * <p>
 * Asking the catalog whether the id resolves to a live object was tried and removed. It cannot
 * distinguish an id a connector made up from an object that was just dropped; "lock first, then
 * discover the object is gone" is the correct pattern, not a defect ({@code
 * TabletChecker.checkOneTable} is written exactly that way), so flagging it would replace clean
 * {@code AlterCancelException} / {@code AnalysisException} paths with a generic internal error; and
 * it is the entire cost of the check — three {@code ConcurrentHashMap} lookups on the FE's hottest
 * metadata path, some of them behind the recycle bin's {@code synchronized} monitor.
 * <p>
 * The bare {@link Locker#lock(long, LockType, long)} entry point is not checked: by that layer a db
 * and a table are indistinguishable. Every external caller goes through the typed entry points
 * mounted here.
 */
public class LockTargetValidator {
    private static final Logger LOG = LogManager.getLogger(LockTargetValidator.class);

    // Grep tokens, not prose: they are the value of the report's kind= field, so they have to survive
    // sort/uniq intact. The human sentence lives in the detail= field.
    static final String KIND_EXTERNAL_CATALOG_DB = "external_catalog_database";
    static final String KIND_PLACEHOLDER_TABLE_ID = "placeholder_table_id";

    private static final String REMEDY =
            "Only objects owned by the internal catalog may be metadata lock targets, because only their "
                    + "metadata is the in-memory state these locks protect. Either drop the lock for this object, "
                    + "or lock a real internal id. A test that locks a synthetic id on purpose should call "
                    + "LockTestUtils.disableLockTargetValidation() and say why";

    private LockTargetValidator() {
    }

    /**
     * Reject a {@link Database} that belongs to an external catalog. Mounted where a typed
     * {@code Database} is available rather than a bare id.
     */
    public static void validateDatabase(Database database) {
        Mode mode = LockInvariantViolations.currentMode();
        if (mode == Mode.OFF) {
            return;
        }

        // The verdict is computed inside the catch-all and reported outside it. Reporting inside
        // would let the IllegalStateException that ERROR mode raises be swallowed by the very
        // catch that is there to absorb faults in the inspected object.
        String detail;
        try {
            detail = describeExternalCatalogDatabase(database);
        } catch (Exception e) {
            // Validation must never be what breaks locking. An earlier revision dereferenced
            // half-constructed metastore state and let an NPE escape into callers that had done
            // nothing wrong.
            LOG.debug("lock target validation skipped, database could not be inspected", e);
            return;
        }

        if (detail != null) {
            LockInvariantViolations.report(KIND_EXTERNAL_CATALOG_DB, detail, REMEDY, mode);
        }
    }

    /**
     * Reject a table id that is a placeholder rather than an id the internal catalog minted.
     */
    public static void validateTableId(Long tableId) {
        Mode mode = LockInvariantViolations.currentMode();
        if (mode == Mode.OFF) {
            return;
        }

        String detail = describePlaceholderTableId(tableId);
        if (detail != null) {
            LockInvariantViolations.report(KIND_PLACEHOLDER_TABLE_ID, detail, REMEDY, mode);
        }
    }

    /**
     * Reject the first placeholder id in a batch. One report per call: every id in the batch shares
     * the call site, so reporting each would land in the same throttling bucket anyway.
     */
    public static void validateTableIds(Collection<Long> tableIds) {
        Mode mode = LockInvariantViolations.currentMode();
        if (mode == Mode.OFF || tableIds == null) {
            return;
        }

        String detail = null;
        for (Long tableId : tableIds) {
            detail = describePlaceholderTableId(tableId);
            if (detail != null) {
                break;
            }
        }

        if (detail != null) {
            LockInvariantViolations.report(KIND_PLACEHOLDER_TABLE_ID, detail, REMEDY, mode);
        }
    }

    /**
     * Both checks for the {@code (database, tableId)} pair the intensive-db-lock API takes.
     */
    public static void validateTableInDatabase(Database database, Long tableId) {
        validateDatabase(database);
        validateTableId(tableId);
    }

    private static String describeExternalCatalogDatabase(Database database) {
        if (database == null) {
            return null;
        }
        String catalogName = database.getCatalogName();
        if (!CatalogMgr.isExternalCatalog(catalogName)) {
            return null;
        }
        return "database " + database.getFullName() + " (id=" + database.getId()
                + ") belongs to external catalog " + catalogName + ", whose ids the connector mints and the "
                + "FE does not own";
    }

    private static String describePlaceholderTableId(Long tableId) {
        if (tableId == null) {
            return "table id is null";
        }
        if (tableId <= 0) {
            // Internal ids come from LocalMetastore#getNextId and are always positive; -1 is the
            // BaseTableInfo default and 0 is the JDBC connector's fixed id.
            return "table id " + tableId + " is a placeholder, not an id minted by the internal catalog";
        }
        return null;
    }
}
