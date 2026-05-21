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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.connector.metastore.MetastoreTable;
import com.starrocks.sql.optimizer.validate.ValidateException;
import com.starrocks.type.Type;
import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.delta.kernel.internal.util.ColumnMapping.COLUMN_MAPPING_MODE_KEY;
import static io.delta.kernel.internal.util.ColumnMapping.COLUMN_MAPPING_MODE_NAME;
import static io.delta.kernel.internal.util.ColumnMapping.COLUMN_MAPPING_MODE_NONE;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DeltaUtilsTest {

    @Test
    public void testCheckTableFeatureSupported() {
        Throwable exception = assertThrows(ValidateException.class, () -> DeltaUtils.checkProtocolAndMetadata(null, null));
        assertThat(exception.getMessage(), containsString("Delta table is missing protocol or metadata information."));
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
    public void testConvertDeltaSnapshotToSRTable(@Mocked SnapshotImpl snapshot, @Mocked Metadata metadata) {
        new Expectations() {
            {
                snapshot.getSchema((Engine) any);
                result = new StructType(Lists.newArrayList(new StructField("col1", IntegerType.INTEGER, true),
                        new StructField("col2", DateType.DATE, true)));
                minTimes = 0;

                snapshot.getMetadata();
                result = metadata;
                minTimes = 0;

                metadata.getDescription();
                result = Optional.empty();
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
                new DeltaLakeSnapshot("db0", "table0", null, snapshot,
                        new MetastoreTable("db1", "table1", "s3://bucket/path/to/table", 123)));
        Assertions.assertEquals(2, deltaLakeTable.getFullSchema().size());
        Assertions.assertEquals("catalog0", deltaLakeTable.getCatalogName());
        Assertions.assertEquals("db0", deltaLakeTable.getCatalogDBName());
        Assertions.assertEquals("table0", deltaLakeTable.getCatalogTableName());
        Assertions.assertEquals("", deltaLakeTable.getComment());
    }

    @Test
    public void testConvertDeltaSnapshotToSRTablePreservesDescription(@Mocked SnapshotImpl snapshot,
                                                                      @Mocked Metadata metadata) {
        new Expectations() {
            {
                snapshot.getSchema((Engine) any);
                result = new StructType(Lists.newArrayList(new StructField("col1", IntegerType.INTEGER, true)));
                minTimes = 0;

                snapshot.getMetadata();
                result = metadata;
                minTimes = 0;

                metadata.getDescription();
                result = Optional.of("my table description");
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
                new DeltaLakeSnapshot("db0", "table0", null, snapshot,
                        new MetastoreTable("db1", "table1", "s3://bucket/path/to/table", 123)));
        Assertions.assertEquals("my table description", deltaLakeTable.getComment());
    }

    @Test
    public void testLoadPartitionColumnNamesEmptyPartitions(@Mocked SnapshotImpl snapshot,
                                                            @Mocked Metadata metadata,
                                                            @Mocked ArrayValue partitionColumns,
                                                            @Mocked ColumnVector columnVector) {
        new Expectations() {
            {
                snapshot.getMetadata();
                result = metadata;

                metadata.getPartitionColumns();
                result = partitionColumns;

                partitionColumns.getSize();
                result = 0;

                partitionColumns.getElements();
                result = columnVector;
            }
        };

        List<String> result = DeltaUtils.loadPartitionColumnNames(snapshot);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testLoadPartitionColumnNamesMultiplePartitions(@Mocked SnapshotImpl snapshot,
                                                               @Mocked Metadata metadata,
                                                               @Mocked ArrayValue partitionColumns,
                                                               @Mocked ColumnVector columnVector) {
        new Expectations() {
            {
                snapshot.getMetadata();
                result = metadata;

                metadata.getPartitionColumns();
                result = partitionColumns;

                partitionColumns.getSize();
                result = 3;

                partitionColumns.getElements();
                result = columnVector;

                columnVector.isNullAt(0);
                result = false;
                columnVector.getString(0);
                result = "year";

                columnVector.isNullAt(1);
                result = false;
                columnVector.getString(1);
                result = "month";

                columnVector.isNullAt(2);
                result = false;
                columnVector.getString(2);
                result = "day";
            }
        };

        List<String> result = DeltaUtils.loadPartitionColumnNames(snapshot);
        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals("year", result.get(0));
        Assertions.assertEquals("month", result.get(1));
        Assertions.assertEquals("day", result.get(2));
    }

    @Test
    public void testLoadPartitionColumnNames_NullPartitionColumn(@Mocked SnapshotImpl snapshot,
                                                                 @Mocked Metadata metadata,
                                                                 @Mocked ArrayValue partitionColumns,
                                                                 @Mocked ColumnVector columnVector) {
        new Expectations() {
            {
                snapshot.getMetadata();
                result = metadata;

                metadata.getPartitionColumns();
                result = partitionColumns;

                partitionColumns.getSize();
                result = 1;

                partitionColumns.getElements();
                result = columnVector;

                columnVector.isNullAt(0);
                result = true;
            }
        };

        Throwable exception = assertThrows(IllegalArgumentException.class, () ->
                DeltaUtils.loadPartitionColumnNames(snapshot));
        assertThat(exception.getMessage(), containsString("Expected a non-null partition column name"));
    }

    @Test
    public void testBuildColumnWithColumnMappingPreservesComment(@Mocked StructField field,
                                                                 @Mocked FieldMetadata fieldMetadata,
                                                                 @Mocked Type type) {
        new Expectations() {
            {
                field.getName();
                result = "col1";

                field.getMetadata();
                result = fieldMetadata;

                fieldMetadata.contains("comment");
                result = true;
                fieldMetadata.get("comment");
                result = "first column comment";

                fieldMetadata.contains(ColumnMapping.COLUMN_MAPPING_ID_KEY);
                result = false;
                fieldMetadata.contains(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY);
                result = false;
            }
        };

        Column column = DeltaUtils.buildColumnWithColumnMapping(field, type, COLUMN_MAPPING_MODE_NONE);

        Assertions.assertEquals("col1", column.getName());
        Assertions.assertEquals("first column comment", column.getComment());
    }

    @Test
    public void testBuildColumnWithColumnMappingNoCommentMetadata(@Mocked StructField field,
                                                                  @Mocked FieldMetadata fieldMetadata,
                                                                  @Mocked Type type) {
        new Expectations() {
            {
                field.getName();
                result = "col1";

                field.getMetadata();
                result = fieldMetadata;

                fieldMetadata.contains("comment");
                result = false;

                fieldMetadata.contains(ColumnMapping.COLUMN_MAPPING_ID_KEY);
                result = false;
                fieldMetadata.contains(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY);
                result = false;
            }
        };

        Column column = DeltaUtils.buildColumnWithColumnMapping(field, type, COLUMN_MAPPING_MODE_NONE);

        Assertions.assertEquals("col1", column.getName());
        Assertions.assertEquals("", column.getComment());
    }

    @Test
    public void testLoadPartitionColumnNamesUpperCasePartitionColumn(@Mocked SnapshotImpl snapshot,
                                                                     @Mocked Metadata metadata,
                                                                     @Mocked ArrayValue partitionColumns,
                                                                     @Mocked ColumnVector columnVector) {
        new Expectations() {
            {
                snapshot.getMetadata();
                result = metadata;

                metadata.getPartitionColumns();
                result = partitionColumns;

                partitionColumns.getSize();
                result = 2;

                partitionColumns.getElements();
                result = columnVector;

                columnVector.isNullAt(0);
                result = false;
                columnVector.getString(0);
                result = "YEAR";

                columnVector.isNullAt(1);
                result = false;
                columnVector.getString(1);
                result = "MONTH";
            }
        };

        List<String> result = DeltaUtils.loadPartitionColumnNames(snapshot);
        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals("YEAR", result.get(0));
        Assertions.assertEquals("MONTH", result.get(1));
    }
}
