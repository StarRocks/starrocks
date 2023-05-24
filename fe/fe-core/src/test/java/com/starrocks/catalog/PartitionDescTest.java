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
import com.starrocks.analysis.TypeDef;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.PartitionDesc;
import org.apache.commons.lang.NotImplementedException;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PartitionDescTest {

    private List<ColumnDef> columnDefs;
    private Map<String, String> otherProperties;
    private PartitionDesc partitionDesc;

    private class PartitionDescChild extends PartitionDesc {

    }

    @Before
    public void setUp() throws AnalysisException {
        ColumnDef id = new ColumnDef("id", TypeDef.create(PrimitiveType.BIGINT));
        this.columnDefs = Lists.newArrayList(id);

        Map<String, String> otherProperties = new HashMap<>();
        otherProperties.put("storage_medium", "SSD");
        this.otherProperties = otherProperties;

        this.partitionDesc = new PartitionDescChild();
    }

    @Test(expected = NotImplementedException.class)
    public void testAnalyzeByColumnDefs() throws AnalysisException {
        this.partitionDesc.analyze(columnDefs, otherProperties);
    }

    @Test(expected = NotImplementedException.class)
    public void testToSql() throws AnalysisException {
        this.partitionDesc.toSql();
    }

    @Test(expected = NotImplementedException.class)
    public void testToPartitionInfo() throws DdlException {
        Column id = new Column("id", Type.BIGINT);
        List<Column> columns = Lists.newArrayList(id);
        Map<String, Long> partitionNameToId = new HashMap<>();
        partitionNameToId.put("p1", 1003L);
        this.partitionDesc.toPartitionInfo(columns, partitionNameToId, false);
        throw new NotImplementedException();
    }

}
