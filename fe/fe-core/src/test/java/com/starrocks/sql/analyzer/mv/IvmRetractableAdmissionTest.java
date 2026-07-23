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

package com.starrocks.sql.analyzer.mv;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.TableName;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.type.IntegerType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Tests for {@link IvmRetractableAdmission#rejectExplicitColumnAliasDerivedTable}: a forwardable derived table
 * with an explicit column alias list ({@code t(a, b)}) must be rejected up front, because exposeKeysAsOutputs
 * appends a hidden __rowid_key_N__ that the sub-query's column-count check would otherwise reject opaquely.
 */
public class IvmRetractableAdmissionTest {

    private static Column keyColumn(String name) {
        Column column = new Column(name, IntegerType.INT, false);
        column.setIsKey(true);
        return column;
    }

    // A forwardable derived table: outer SELECT ... FROM (SELECT id FROM <cloud-native PK base>) t.
    private void mockForwardableDerivedTable(SelectRelation outer, SubqueryRelation subquery, QueryStatement qs,
                                             SelectRelation inner, TableRelation tableRelation, OlapTable table) {
        new Expectations() {
            {
                outer.getRelation();
                result = subquery;
                minTimes = 0;
                subquery.getQueryStatement();
                result = qs;
                minTimes = 0;
                qs.getQueryRelation();
                result = inner;
                minTimes = 0;
                inner.getRelation();
                result = tableRelation;
                minTimes = 0;
                tableRelation.getTable();
                result = table;
                minTimes = 0;
                tableRelation.getResolveTableName();
                result = new TableName("db", "pk_t");
                minTimes = 0;
                table.isCloudNativeTableOrMaterializedView();
                result = true;
                minTimes = 0;
                table.getKeysType();
                result = KeysType.PRIMARY_KEYS;
                minTimes = 0;
                table.getBaseSchema();
                result = List.of(keyColumn("id"));
                minTimes = 0;
            }
        };
    }

    @Test
    public void testRejectsExplicitColumnAliasOnForwardableDerivedTable(
            @Mocked SelectRelation outer, @Mocked SubqueryRelation subquery, @Mocked QueryStatement qs,
            @Mocked SelectRelation inner, @Mocked TableRelation tableRelation, @Mocked OlapTable table) {
        mockForwardableDerivedTable(outer, subquery, qs, inner, tableRelation, table);
        new Expectations() {
            {
                subquery.getExplicitColumnNames();
                result = Lists.newArrayList("a", "b");
                minTimes = 0;
            }
        };
        Assertions.assertThrows(SemanticException.class,
                () -> IvmRetractableAdmission.rejectExplicitColumnAliasDerivedTable(outer),
                "a forwardable derived table with an explicit column alias list must be rejected");
    }

    @Test
    public void testAllowsForwardableDerivedTableWithoutColumnAliasList(
            @Mocked SelectRelation outer, @Mocked SubqueryRelation subquery, @Mocked QueryStatement qs,
            @Mocked SelectRelation inner, @Mocked TableRelation tableRelation, @Mocked OlapTable table) {
        mockForwardableDerivedTable(outer, subquery, qs, inner, tableRelation, table);
        // subquery.getExplicitColumnNames() defaults to null (no t(a, b) list) -> not rejected.
        Assertions.assertDoesNotThrow(() -> IvmRetractableAdmission.rejectExplicitColumnAliasDerivedTable(outer));
    }
}
