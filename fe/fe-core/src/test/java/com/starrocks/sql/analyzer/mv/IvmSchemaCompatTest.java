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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

public class IvmSchemaCompatTest {

    private static Column hidden(String name, Type type) {
        Column c = new Column(name, type);
        c.setIsHidden(true);
        return c;
    }

    private static Field field(Type type) {
        return new Field(null, type, null, null, true, true);
    }

    private static MaterializedView mvWithColumns(List<Column> columns) {
        MaterializedView mv = Mockito.mock(MaterializedView.class);
        Mockito.when(mv.getOrderedOutputColumns(true)).thenReturn(columns);
        return mv;
    }

    @Test
    public void testArityMismatchThrows() {
        List<Column> stored = Arrays.asList(
                hidden("__ROW_ID__", IntegerType.BIGINT),
                new Column("k", IntegerType.INT),
                new Column("cnt", IntegerType.BIGINT),
                hidden("__AGG_STATE_count__", IntegerType.BIGINT));
        MaterializedView mv = mvWithColumns(stored);
        List<Field> derived = Arrays.asList(field(IntegerType.INT), field(IntegerType.BIGINT));
        Assertions.assertThrows(SemanticException.class, () -> IvmSchemaCompat.compare(derived, mv));
    }

    @Test
    public void testCompatiblePasses() {
        List<Column> stored = Arrays.asList(
                hidden("__ROW_ID__", IntegerType.BIGINT),
                new Column("k", IntegerType.INT),
                new Column("cnt", IntegerType.BIGINT),
                hidden("__AGG_STATE_count__", IntegerType.BIGINT));
        MaterializedView mv = mvWithColumns(stored);
        List<Field> derived = Arrays.asList(
                field(IntegerType.BIGINT),
                field(IntegerType.INT),
                field(IntegerType.BIGINT),
                field(IntegerType.BIGINT));
        Assertions.assertDoesNotThrow(() -> IvmSchemaCompat.compare(derived, mv));
    }
}
