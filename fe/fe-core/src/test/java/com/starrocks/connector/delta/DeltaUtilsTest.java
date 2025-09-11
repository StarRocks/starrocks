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
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.optimizer.validate.ValidateException;
import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.TransactionBuilder;
import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.CheckpointAlreadyExistsException;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.TableImpl;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.List;

import static io.delta.kernel.internal.util.ColumnMapping.COLUMN_MAPPING_MODE_KEY;
import static io.delta.kernel.internal.util.ColumnMapping.COLUMN_MAPPING_MODE_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
    public void testConvertDeltaToSRTableWithException1() {
        expectedEx.expect(SemanticException.class);
        expectedEx.expectMessage("Failed to find Delta table for catalog.db.tbl");

        new MockUp<Table>() {
            @mockit.Mock
            public Table forPath(Engine deltaEngine, String path) throws TableNotFoundException {
                throw new TableNotFoundException("Table not found");
            }
        };

        DeltaUtils.convertDeltaToSRTable("catalog", "db", "tbl", "path",
                DeltaLakeEngine.create(new Configuration()), 0);
    }

    @Test
    public void testConvertDeltaToSRTableWithException2() {
        expectedEx.expect(SemanticException.class);
        expectedEx.expectMessage("Failed to get latest snapshot for catalog.db.tbl");
        Table table = new Table() {
            public Table forPath(Engine engine, String path) {
                return this;
            }

            @Override
            public String getPath(Engine engine) {
                return null;
            }

            @Override
            public SnapshotImpl getLatestSnapshot(Engine engine) {
                throw new RuntimeException("Failed to get latest snapshot");
            }

            @Override
            public Snapshot getSnapshotAsOfVersion(Engine engine, long versionId) throws TableNotFoundException {
                return null;
            }

            @Override
            public Snapshot getSnapshotAsOfTimestamp(Engine engine, long millisSinceEpochUTC)
                    throws TableNotFoundException {
                return null;
            }

            @Override
            public TransactionBuilder createTransactionBuilder(Engine engine, String engineInfo, Operation operation) {
                return null;
            }

            @Override
            public void checkpoint(Engine engine, long version)
                    throws TableNotFoundException, CheckpointAlreadyExistsException, IOException {
            }
        };

        new MockUp<TableImpl>() {
            @Mock
            public Table forPath(Engine engine, String path) {
                return table;
            }
        };

        DeltaUtils.convertDeltaToSRTable("catalog", "db", "tbl", "path",
                DeltaLakeEngine.create(new Configuration()), 0);
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
