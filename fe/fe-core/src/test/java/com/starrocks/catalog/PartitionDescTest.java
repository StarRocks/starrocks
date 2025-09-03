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
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.expression.TypeDef;
import org.apache.commons.lang.NotImplementedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class PartitionDescTest {

    private List<ColumnDef> columnDefs;
    private Map<String, String> otherProperties;
    private PartitionDesc partitionDesc;

    private class PartitionDescChild extends PartitionDesc {

    }

    @BeforeEach
    public void setUp() throws AnalysisException {
        ColumnDef id = new ColumnDef("id", TypeDef.create(PrimitiveType.BIGINT));
        this.columnDefs = Lists.newArrayList(id);

        Map<String, String> otherProperties = new HashMap<>();
        otherProperties.put("storage_medium", "SSD");
        this.otherProperties = otherProperties;

        this.partitionDesc = new PartitionDescChild();
    }

    @Test
    public void testToSql() {
        assertThrows(NotImplementedException.class, () -> this.partitionDesc.toSql());
    }

    @Test
    public void testPartitionInfoBuilder() {
        // Since toPartitionInfo method was removed, we now use PartitionInfoBuilder directly
        // We expect DdlException for unsupported partition types
        assertThrows(DdlException.class, () -> {
            Column id = new Column("id", Type.BIGINT);
            List<Column> columns = Lists.newArrayList(id);
            Map<String, Long> partitionNameToId = new HashMap<>();
            partitionNameToId.put("p1", 1003L);
            // Use PartitionInfoBuilder directly instead of toPartitionInfo
            PartitionInfoBuilder.build(this.partitionDesc, columns, partitionNameToId, false);
        });
    }

}
