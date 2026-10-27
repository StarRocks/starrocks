package com.starrocks.connector.statistics;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Table;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.qe.ConnectContext;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StatisticsUtilsTest {

    @Test
    void testGetTableNameByUUIDWithJdbcTableUUID() {
        String tableUUID = "oracle_repro.table.tables";

        List<String> tableName = StatisticsUtils.getTableNameByUUID(tableUUID);

        assertEquals(
                ImmutableList.of("oracle_repro", "table", "tables"),
                tableName
        );
    }

    @Test
    void testGetTableNameByUUIDWithFourPartUUID() {
        String tableUUID = "catalog.db.table.uuid";

        List<String> tableName = StatisticsUtils.getTableNameByUUID(tableUUID);

        assertEquals(
                ImmutableList.of("catalog", "db", "table"),
                tableName
        );
    }
}