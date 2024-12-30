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

package com.starrocks.connector.delta;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.sql.optimizer.validate.ValidateException;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

import static io.delta.kernel.internal.util.ColumnMapping.COLUMN_MAPPING_MODE_KEY;
import static io.delta.kernel.internal.util.ColumnMapping.COLUMN_MAPPING_MODE_NAME;
import static io.delta.kernel.internal.util.ColumnMapping.COLUMN_MAPPING_MODE_NONE;

public class DeltaUtilsTest {
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testCheckTableFeatureSupported() {
        expectedEx.expect(ValidateException.class);
        expectedEx.expectMessage("Delta table is missing protocol or metadata information.");
        DeltaUtils.checkProtocolAndMetadata(null, null);
    }

    @Test
    public void testCheckTableFeatureSupported2(@Mocked Metadata metadata) {
        new Expectations(metadata) {
            {
                metadata.getConfiguration();
                result = ImmutableMap.of(COLUMN_MAPPING_MODE_KEY, COLUMN_MAPPING_MODE_NAME);
                minTimes = 0;
            }
        };

        DeltaUtils.checkProtocolAndMetadata(new Protocol(3, 7, Lists.newArrayList(),
                Lists.newArrayList()), metadata);
    }

    @Test
    public void testConvertDeltaSnapshotToSRTable(@Mocked SnapshotImpl snapshot) {
        new Expectations() {
            {
                snapshot.getSchema((Engine) any);
                result = new StructType(Lists.newArrayList(new StructField("col1", IntegerType.INTEGER, true),
                        new StructField("col2", DateType.DATE, true)));
                minTimes = 0;
            }
        };

        new MockUp<ColumnMapping>() {
            @Mock
            public String getColumnMappingMode(Configuration configuration) {
                return COLUMN_MAPPING_MODE_NONE;
            }
        };

        new MockUp<DeltaUtils>() {
            @Mock
            public List<String> loadPartitionColumnNames(SnapshotImpl snapshot) {
                return Lists.newArrayList();
            }
        };

        DeltaLakeTable deltaLakeTable = DeltaUtils.convertDeltaSnapshotToSRTable("catalog0",
                new DeltaLakeSnapshot("db0", "table0", null, snapshot, 123, "path"));
        Assert.assertEquals(2, deltaLakeTable.getFullSchema().size());
        Assert.assertEquals("catalog0", deltaLakeTable.getCatalogName());
        Assert.assertEquals("db0", deltaLakeTable.getCatalogDBName());
        Assert.assertEquals("table0", deltaLakeTable.getCatalogTableName());
    }
}
