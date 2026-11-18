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

package com.starrocks.alter;

import com.starrocks.catalog.Column;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

public class AlterHelperTest {

    private static Column shadow(String name, Type type) {
        return new Column(SchemaChangeHandler.SHADOW_NAME_PREFIX + name, type);
    }

    @Test
    public void testCollectStatsInvalidatedColumns() {
        List<Column> oldColumns = List.of(
                new Column("k1", IntegerType.INT),
                new Column("c_str", new VarcharType(64)),
                new Column("c_int", IntegerType.INT),
                new Column("c_dt", DateType.DATETIME),
                new Column("c_vc", new VarcharType(10)));

        List<Column> newColumns = List.of(
                new Column("k1", IntegerType.INT),
                // varchar -> bigint: the stored lexicographic min/max are invalid bigint boundaries
                shadow("c_str", IntegerType.BIGINT),
                // int -> bigint: monotonic widening, statistics stay valid
                shadow("c_int", IntegerType.BIGINT),
                // datetime -> date: truncating conversion, statistics invalid
                shadow("c_dt", DateType.DATE),
                // varchar length increase: ordering unchanged, statistics stay valid
                shadow("c_vc", new VarcharType(20)));

        Set<String> result = AlterHelper.collectStatsInvalidatedColumns(oldColumns, newColumns);
        Assertions.assertEquals(2, result.size());
        Assertions.assertTrue(result.contains("c_str"));
        Assertions.assertTrue(result.contains("c_dt"));
    }

    @Test
    public void testCollectStatsInvalidatedColumnsIgnoresNonTypeChanges() {
        // columns without the shadow prefix never changed their type and are never reported
        List<Column> oldColumns = List.of(new Column("c1", IntegerType.INT), new Column("c2", new VarcharType(8)));
        List<Column> newColumns = List.of(new Column("c1", IntegerType.INT), new Column("c2", new VarcharType(8)));
        Assertions.assertTrue(AlterHelper.collectStatsInvalidatedColumns(oldColumns, newColumns).isEmpty());

        // a shadow column whose origin no longer exists is ignored as well
        List<Column> onlyShadow = List.of(shadow("gone", IntegerType.BIGINT));
        Assertions.assertTrue(AlterHelper.collectStatsInvalidatedColumns(oldColumns, onlyShadow).isEmpty());
    }
}
