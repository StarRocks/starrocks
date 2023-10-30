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

import com.google.common.collect.Lists;
import com.starrocks.connector.ColumnTypeConverter;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.paimon.table.AbstractFileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class PaimonTableTest {

    @Test
    public void testPartitionKeys(@Mocked AbstractFileStoreTable paimonNativeTable) {
        RowType rowType =
                RowType.builder().field("a", DataTypes.INT()).field("b", DataTypes.INT()).field("c", DataTypes.INT())
                        .build();
        List<DataField> fields = rowType.getFields();
        List<Column> fullSchema = new ArrayList<>(fields.size());
        ArrayList<String> partitions = Lists.newArrayList("b", "c");

        ArrayList<Column> expections = new ArrayList<>();
        for (DataField field : fields) {
            String fieldName = field.name();
            DataType type = field.type();
            Type fieldType = ColumnTypeConverter.fromPaimonType(type);
            Column column = new Column(fieldName, fieldType, true);
            fullSchema.add(column);
            if (partitions.contains(fieldName)) {
                expections.add(column);
            }
        }
        new Expectations() {
            {
                paimonNativeTable.rowType();
                result = rowType;
                paimonNativeTable.partitionKeys();
                result = partitions;
            }
        };
        PaimonTable paimonTable = new PaimonTable("testCatalog", "testDB", "testTable", fullSchema, "filesystem", null,
                "file:///home/wgcn", paimonNativeTable);
        List<String> keys = new ArrayList<>();
        List<Column> partitionColumns = paimonTable.getPartitionColumns();
        Assertions.assertThat(partitionColumns).hasSameElementsAs(expections);
    }

}