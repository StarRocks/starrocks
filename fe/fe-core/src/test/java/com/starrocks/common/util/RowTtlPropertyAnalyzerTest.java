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

package com.starrocks.common.util;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.Config;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link RowTtlPropertyAnalyzer} against a mocked table, which is enough because the class
 * only reads a table's storage kind, keys type and columns.
 */
public class RowTtlPropertyAnalyzerTest {
    private static final String EXPIRE_AT = PropertyAnalyzer.PROPERTIES_ROW_TTL_EXPIRE_AT;
    private static final String CHECK_INTERVAL = PropertyAnalyzer.PROPERTIES_ROW_TTL_CHECK_INTERVAL_SECOND;
    private static final String TIME_ZONE = PropertyAnalyzer.PROPERTIES_ROW_TTL_TIME_ZONE;

    private boolean savedEnableRowTtl;
    private OlapTable table;

    @BeforeEach
    public void setUp() {
        savedEnableRowTtl = Config.enable_row_ttl;
        Config.enable_row_ttl = true;
        table = primaryKeyLakeTable();
    }

    @AfterEach
    public void tearDown() {
        Config.enable_row_ttl = savedEnableRowTtl;
    }

    private static OlapTable primaryKeyLakeTable() {
        OlapTable t = mock(OlapTable.class);
        when(t.isCloudNativeTable()).thenReturn(true);
        when(t.getKeysType()).thenReturn(KeysType.PRIMARY_KEYS);
        when(t.getName()).thenReturn("t");
        when(t.getColumn("expire_at")).thenReturn(new Column("expire_at", DateType.DATETIME));
        when(t.getColumn("event_date")).thenReturn(new Column("event_date", DateType.DATE));
        when(t.getColumn("created_at")).thenReturn(new Column("created_at", IntegerType.BIGINT));
        when(t.getColumn("created_ms")).thenReturn(new Column("created_ms", IntegerType.INT));
        when(t.getColumn("name")).thenReturn(new Column("name", VarcharType.VARCHAR));
        Column autoInc = new Column("id", IntegerType.BIGINT);
        autoInc.setIsAutoIncrement(true);
        when(t.getColumn("id")).thenReturn(autoInc);
        return t;
    }

    private static Map<String, String> props(String... keyValues) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            map.put(keyValues[i], keyValues[i + 1]);
        }
        return map;
    }

    private void accept(String... keyValues) {
        RowTtlPropertyAnalyzer.analyze(table, new HashMap<>(), props(keyValues));
    }

    private String reject(String... keyValues) {
        SemanticException e = assertThrows(SemanticException.class,
                () -> RowTtlPropertyAnalyzer.analyze(table, new HashMap<>(), props(keyValues)));
        return e.getMessage();
    }

    private String rejectOnConfigured(Map<String, String> current, String... keyValues) {
        SemanticException e = assertThrows(SemanticException.class,
                () -> RowTtlPropertyAnalyzer.analyze(table, current, props(keyValues)));
        return e.getMessage();
    }

    @Test
    public void testTableEligibility() {
        OlapTable sharedNothing = mock(OlapTable.class);
        when(sharedNothing.isCloudNativeTable()).thenReturn(false);
        SemanticException e = assertThrows(SemanticException.class, () -> RowTtlPropertyAnalyzer.analyze(
                sharedNothing, new HashMap<>(), props(EXPIRE_AT, "expire_at")));
        assertTrue(e.getMessage().contains("shared-data"), e.getMessage());

        OlapTable duplicateKey = mock(OlapTable.class);
        when(duplicateKey.isCloudNativeTable()).thenReturn(true);
        when(duplicateKey.getKeysType()).thenReturn(KeysType.DUP_KEYS);
        e = assertThrows(SemanticException.class, () -> RowTtlPropertyAnalyzer.analyze(
                duplicateKey, new HashMap<>(), props(EXPIRE_AT, "expire_at")));
        assertTrue(e.getMessage().contains("primary key"), e.getMessage());

        // A statement carrying no row TTL key is none of this class's business, whatever the table is.
        RowTtlPropertyAnalyzer.analyze(sharedNothing, new HashMap<>(), props("replication_num", "1"));
    }

    @Test
    public void testAdmissionGate() {
        Config.enable_row_ttl = false;
        Map<String, String> unconfigured = new HashMap<>();
        SemanticException e = assertThrows(SemanticException.class,
                () -> RowTtlPropertyAnalyzer.checkAdmission(unconfigured, props(EXPIRE_AT, "expire_at")));
        assertTrue(e.getMessage().contains("enable_row_ttl"), e.getMessage());

        // The gate guards the entrance only: a table that already has row TTL may still be changed.
        Map<String, String> configured = props(EXPIRE_AT, "expire_at");
        RowTtlPropertyAnalyzer.checkAdmission(configured, props(EXPIRE_AT, "event_date"));
        RowTtlPropertyAnalyzer.checkAdmission(configured, props(CHECK_INTERVAL, "3600"));
        // And it says nothing about values, which analyze() judges whatever the switch is.
        RowTtlPropertyAnalyzer.analyze(table, unconfigured, props(EXPIRE_AT, "expire_at"));
    }

    @Test
    public void testExpirationExpressionIsRequiredFirst() {
        assertTrue(reject(CHECK_INTERVAL, "3600").contains(EXPIRE_AT));
        assertTrue(reject(TIME_ZONE, "UTC").contains(EXPIRE_AT));
        // All three in one statement is how a table is configured the first time.
        accept(EXPIRE_AT, "expire_at", CHECK_INTERVAL, "3600", TIME_ZONE, "UTC");
    }

    @Test
    public void testEmptyValueIsRejectedForEveryKey() {
        assertTrue(reject(EXPIRE_AT, "").contains("empty"));
        Map<String, String> configured = props(EXPIRE_AT, "expire_at");
        assertTrue(rejectOnConfigured(configured, CHECK_INTERVAL, "").contains("empty"));
        assertTrue(rejectOnConfigured(configured, TIME_ZONE, "  ").contains("empty"));
    }

    @Test
    public void testAcceptedExpressionForms() {
        accept(EXPIRE_AT, "expire_at");
        accept(EXPIRE_AT, "event_date");
        accept(EXPIRE_AT, "expire_at + INTERVAL 7 DAY");
        accept(EXPIRE_AT, "to_datetime(created_at)");
        accept(EXPIRE_AT, "to_datetime(created_ms, 3)");
        accept(EXPIRE_AT, "to_datetime(created_at, 0) + INTERVAL 30 DAY");
        accept(EXPIRE_AT, "`expire_at`");
    }

    @Test
    public void testRejectedExpressionShapes() {
        assertTrue(reject(EXPIRE_AT, "INTERVAL 7 DAY + expire_at").contains("INTERVAL"));
        assertTrue(reject(EXPIRE_AT, "date_add(expire_at, INTERVAL 7 DAY)").contains("INTERVAL"));
        assertTrue(reject(EXPIRE_AT, "expire_at - INTERVAL 7 DAY").contains("INTERVAL"));
        assertTrue(reject(EXPIRE_AT, "expire_at > now()").contains(EXPIRE_AT));
        assertTrue(reject(EXPIRE_AT, "from_unixtime(created_at)").contains(EXPIRE_AT));
        assertTrue(reject(EXPIRE_AT, "t.expire_at").contains("table name"));
        assertTrue(reject(EXPIRE_AT, "no_such_column").contains("does not exist"));
    }

    @Test
    public void testColumnTypeHasToMatchTheForm() {
        // A bare integer column has to be told what to_datetime is for, not just to use it.
        String message = reject(EXPIRE_AT, "created_at");
        assertTrue(message.contains("to_datetime"), message);
        assertTrue(message.contains("Unix timestamp"), message);

        assertTrue(reject(EXPIRE_AT, "name").contains("DATE"));
        assertTrue(reject(EXPIRE_AT, "to_datetime(expire_at)").contains("INT"));
        assertTrue(reject(EXPIRE_AT, "id").contains("AUTO_INCREMENT"));
    }

    @Test
    public void testToDatetimeScale() {
        accept(EXPIRE_AT, "to_datetime(created_at, 0)");
        accept(EXPIRE_AT, "to_datetime(created_at, 3)");
        accept(EXPIRE_AT, "to_datetime(created_at, 6)");
        // 2 would make the function return NULL, so the table would look configured and never
        // expire a row; -3 is worse, it is silently taken as seconds.
        assertTrue(reject(EXPIRE_AT, "to_datetime(created_at, 2)").contains("scale"));
        assertTrue(reject(EXPIRE_AT, "to_datetime(created_at, -3)").contains("scale"));
        assertTrue(reject(EXPIRE_AT, "to_datetime(created_at, created_ms)").contains("integer literal"));
    }

    @Test
    public void testToDatetimeNtzPointsAtTheSupportedSpelling() {
        String message = reject(EXPIRE_AT, "to_datetime_ntz(created_at)");
        assertTrue(message.contains("to_datetime_ntz"), message);
        assertTrue(message.contains(TIME_ZONE), message);
        assertTrue(message.contains("UTC"), message);
    }

    @Test
    public void testIntervalUnitAndAmount() {
        for (String unit : new String[] {"YEAR", "QUARTER", "MONTH", "WEEK", "DAY", "HOUR", "MINUTE", "SECOND"}) {
            accept(EXPIRE_AT, "expire_at + INTERVAL 1 " + unit);
        }
        assertTrue(reject(EXPIRE_AT, "expire_at + INTERVAL 1 MILLISECOND").contains("MILLISECOND"));
        assertTrue(reject(EXPIRE_AT, "expire_at + INTERVAL 1 MICROSECOND").contains("MICROSECOND"));
        assertTrue(reject(EXPIRE_AT, "expire_at + INTERVAL 0 DAY").contains("out of range"));
        assertTrue(reject(EXPIRE_AT, "expire_at + INTERVAL 2147483648 DAY").contains("out of range"));
        accept(EXPIRE_AT, "expire_at + INTERVAL 2147483647 DAY");
    }

    @Test
    public void testCheckIntervalSecond() {
        accept(EXPIRE_AT, "expire_at", CHECK_INTERVAL, "1");
        accept(EXPIRE_AT, "expire_at", CHECK_INTERVAL, "86400");
        assertTrue(reject(EXPIRE_AT, "expire_at", CHECK_INTERVAL, "1 hour").contains("integer"));
        assertTrue(reject(EXPIRE_AT, "expire_at", CHECK_INTERVAL, "0").contains("greater than 0"));
        assertTrue(reject(EXPIRE_AT, "expire_at", CHECK_INTERVAL, "-1").contains("greater than 0"));
    }

    @Test
    public void testTimeZone() {
        accept(EXPIRE_AT, "expire_at", TIME_ZONE, "UTC");
        accept(EXPIRE_AT, "expire_at", TIME_ZONE, "Asia/Shanghai");
        accept(EXPIRE_AT, "expire_at", TIME_ZONE, "+08:00");
        accept(EXPIRE_AT, "expire_at", TIME_ZONE, "CST");
        assertTrue(reject(EXPIRE_AT, "expire_at", TIME_ZONE, "Mars/Olympus").contains("time zone"));
    }

    @Test
    public void testExpireAtColumn() {
        assertEquals("expire_at", RowTtlPropertyAnalyzer.expireAtColumn("expire_at").orElse(null));
        assertEquals("expire_at", RowTtlPropertyAnalyzer.expireAtColumn("expire_at + INTERVAL 7 DAY").orElse(null));
        assertEquals("created_at", RowTtlPropertyAnalyzer.expireAtColumn("to_datetime(created_at, 3)").orElse(null));
        assertEquals("created_at",
                RowTtlPropertyAnalyzer.expireAtColumn("to_datetime(created_at) + INTERVAL 1 HOUR").orElse(null));
        assertFalse(RowTtlPropertyAnalyzer.expireAtColumn(null).isPresent());
        assertFalse(RowTtlPropertyAnalyzer.expireAtColumn("").isPresent());
        // A value that never passed analyze() must not blow up a caller's DDL.
        assertFalse(RowTtlPropertyAnalyzer.expireAtColumn("!!! not an expression").isPresent());
    }

    @Test
    public void testCheckColumnNotUsedByRowTtl() {
        TableProperty property = new TableProperty(props(EXPIRE_AT, "expire_at + INTERVAL 7 DAY"));
        when(table.getTableProperty()).thenReturn(property);

        SemanticException e = assertThrows(SemanticException.class,
                () -> RowTtlPropertyAnalyzer.checkColumnNotUsedByRowTtl(table, "expire_at", "dropped"));
        assertTrue(e.getMessage().contains("DROP ROW TTL"), e.getMessage());
        assertTrue(e.getMessage().contains("dropped"), e.getMessage());
        // Case matters no more here than it does anywhere else a column is named.
        assertThrows(SemanticException.class,
                () -> RowTtlPropertyAnalyzer.checkColumnNotUsedByRowTtl(table, "EXPIRE_AT", "renamed"));

        RowTtlPropertyAnalyzer.checkColumnNotUsedByRowTtl(table, "created_at", "dropped");

        OlapTable withoutRowTtl = mock(OlapTable.class);
        when(withoutRowTtl.getTableProperty()).thenReturn(new TableProperty(new HashMap<>()));
        RowTtlPropertyAnalyzer.checkColumnNotUsedByRowTtl(withoutRowTtl, "expire_at", "dropped");
    }

    @Test
    public void testUnknownRowTtlKeyIsRejected() {
        // row_ttl_check_interval is what the property was called two days ago, so it is what someone
        // working from an early draft or guessing at the name writes. Collected by prefix and asked
        // for by nobody, it would otherwise be stored and never read: the table looks configured
        // while it quietly runs on the cluster default.
        String message = reject(EXPIRE_AT, "expire_at", "row_ttl_check_interval", "3600");
        assertTrue(message.contains("row_ttl_check_interval"), message);
        assertTrue(message.contains(CHECK_INTERVAL), message);

        assertTrue(reject(EXPIRE_AT, "expire_at", "row_ttl_check_intervall", "60")
                .contains("row_ttl_check_intervall"));
        assertTrue(reject(EXPIRE_AT, "expire_at", "row_ttl_timezone", "UTC").contains("row_ttl_timezone"));
        // An unknown key on its own is named as unknown rather than blamed on the missing expression.
        assertTrue(reject("row_ttl_whatever", "1").contains("row_ttl_whatever"));
        // A property outside the prefix is none of this class's business.
        accept(EXPIRE_AT, "expire_at");
    }

    @Test
    public void testContainsRowTtlProperty() {
        assertFalse(RowTtlPropertyAnalyzer.containsRowTtlProperty(null));
        assertFalse(RowTtlPropertyAnalyzer.containsRowTtlProperty(props("replication_num", "1")));
        assertTrue(RowTtlPropertyAnalyzer.containsRowTtlProperty(props(EXPIRE_AT, "expire_at")));
        assertTrue(RowTtlPropertyAnalyzer.containsRowTtlProperty(props(TIME_ZONE, "UTC")));
    }
}
