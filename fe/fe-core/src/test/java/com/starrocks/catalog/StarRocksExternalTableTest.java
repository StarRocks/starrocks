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

package com.starrocks.catalog;

import com.google.common.collect.ImmutableList;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * The UUID contract required by StatisticsUtils.getTableByUUID: exactly four
 * dot-separated segments, resolvable back to catalog.db.table, stable across
 * resolutions of the same remote table, and different for a new incarnation.
 */
public class StarRocksExternalTableTest {

    private static final List<Column> SCHEMA = ImmutableList.of(new Column("k", IntegerType.INT, true));

    private static StarRocksExternalTable table(long localId, long remoteTableId) {
        return table(localId, "sr_catalog", "db1", "tbl1", 7, remoteTableId);
    }

    private static StarRocksExternalTable table(long localId, String catalog, String db, String name,
                                                long schemaVersion, long remoteTableId) {
        return new StarRocksExternalTable(localId, catalog, db, name, SCHEMA, schemaVersion, remoteTableId,
                ImmutableList.of(), 0, null);
    }

    @Test
    public void testUuidIsFourSegmentsOfCatalogDbTableAndRemoteTableId() {
        StarRocksExternalTable table = table(100000001L, 10086L);

        Assertions.assertEquals(10086L, table.getRemoteTableId());
        Assertions.assertEquals("sr_catalog.db1.tbl1.10086", table.getUUID());
        // getTableByUUID hard-fails its Preconditions.checkState below four segments.
        Assertions.assertEquals(4, table.getUUID().split("\\.").length);
    }

    @Test
    public void testUuidIsStableAcrossResolutionsDespiteFreshLocalIds() {
        // The connector mints a new local id on every getTable(); the identity must not follow it,
        // otherwise getTableByUUID's equality check can never match a cached key.
        Assertions.assertEquals(table(100000001L, 10086L).getUUID(),
                table(100000042L, 10086L).getUUID());
    }

    @Test
    public void testUuidSurvivesSchemaEvolution() {
        // ALTER bumps the remote schema version but does not make it a different table,
        // so the identity -- and with it any statistics keyed on it -- must not change.
        Assertions.assertEquals(table(1L, "sr_catalog", "db1", "tbl1", 7, 10086L).getUUID(),
                table(2L, "sr_catalog", "db1", "tbl1", 8, 10086L).getUUID());
    }

    @Test
    public void testUuidChangesWhenRemoteTableIsRecreated() {
        // A recreated table keeps its name and can keep its create time (second granularity),
        // but never its id.
        Assertions.assertNotEquals(table(1L, 10086L).getUUID(), table(1L, 10087L).getUUID());
    }

    @Test
    public void testDottedNamesStayWithinFourSegments() {
        // Dots are legal in database and table names (FeNameFormat only rejects NUL) and would
        // otherwise push the UUID past four segments.
        StarRocksExternalTable dotted = table(1L, "sr_catalog", "my.db", "my.tbl", 7, 10086L);

        Assertions.assertEquals("sr_catalog.my_db.my_tbl.10086", dotted.getUUID());
        Assertions.assertEquals(4, dotted.getUUID().split("\\.").length);
        // The escaping is lossy, so it can alias a real table -- but only on the name segments:
        // the remote table id still separates the two identities.
        Assertions.assertNotEquals(dotted.getUUID(),
                table(1L, "sr_catalog", "my_db", "my_tbl", 7, 10087L).getUUID());
    }
}
