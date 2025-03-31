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

package com.starrocks.connector.iceberg;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.starrocks.connector.iceberg.MockIcebergMetadata.MOCKED_PARTITIONED_TRANSFORMS_DB_NAME;
import static com.starrocks.connector.iceberg.MockIcebergMetadata.getStarRocksHome;

public class MIcebergTableMeta {

    // multi partition columns: id, data, hour(ts)
    public static final MIcebergTable T0_MULTI_HOUR = new MIcebergTable() {
        @Override
        public String getTableName() {
            return "t0_multi_hour";
        }

        @Override
        public TestTables.TestTable getTestTable(Schema schema) throws IOException {
            PartitionSpec spec =
                    PartitionSpec.builderFor(schema)
                            .identity("id")
                            .identity("data")
                            .hour("ts")
                            .build();
            String tableName = getTableName();
            return TestTables.create(
                    new File(getStarRocksHome() + "/" + MOCKED_PARTITIONED_TRANSFORMS_DB_NAME + "/"
                            + tableName), tableName, schema, spec, 1);
        }

        @Override
        public List<String> getTransformTablePartitionNames(String tblName) {
            return Lists.newArrayList("id=1/data=a/ts_hour=2022-01-01-00", "id=2/data=a/ts_year=2022-01-01-01");
        }
    };

    // multi partition columns: id, data, day(ts)
    public static final MIcebergTable T0_MULTI_DAY = new MIcebergTable() {
        @Override
        public String getTableName() {
            return "t0_multi_day";
        }

        @Override
        public TestTables.TestTable getTestTable(Schema schema) throws IOException {
            PartitionSpec spec =
                    PartitionSpec.builderFor(schema)
                            .identity("id")
                            .identity("data")
                            .day("ts")
                            .build();
            String tableName = getTableName();
            return TestTables.create(
                    new File(getStarRocksHome() + "/" + MOCKED_PARTITIONED_TRANSFORMS_DB_NAME + "/"
                            + tableName), tableName, schema, spec, 1);
        }

        @Override
        public List<String> getTransformTablePartitionNames(String tblName) {
            return Lists.newArrayList("id=1/data=a/ts_day=2022-01-01", "id=2/data=a/ts_day=2022-01-02");
        }
    };

    // multi partition columns: id, data, month(ts)
    public static final MIcebergTable T0_MULTI_MONTH = new MIcebergTable() {
        @Override
        public String getTableName() {
            return "t0_multi_month";
        }

        @Override
        public TestTables.TestTable getTestTable(Schema schema) throws IOException {
            PartitionSpec spec =
                    PartitionSpec.builderFor(schema)
                            .identity("id")
                            .identity("data")
                            .month("ts")
                            .build();
            String tableName = getTableName();
            return TestTables.create(
                    new File(getStarRocksHome() + "/" + MOCKED_PARTITIONED_TRANSFORMS_DB_NAME + "/"
                            + tableName), tableName, schema, spec, 1);
        }

        @Override
        public List<String> getTransformTablePartitionNames(String tblName) {
            return Lists.newArrayList("id=1/data=a/ts_month=2022-01", "id=2/data=a/ts_month=2022-02");
        }
    };

    // multi partition columns: id, data, year(ts)
    public static final MIcebergTable T0_MULTI_YEAR = new MIcebergTable() {
        @Override
        public String getTableName() {
            return "t0_multi_year";
        }

        @Override
        public TestTables.TestTable getTestTable(Schema schema) throws IOException {
            PartitionSpec spec =
                    PartitionSpec.builderFor(schema)
                            .identity("id")
                            .identity("data")
                            .year("ts")
                            .build();
            String tableName = getTableName();
            return TestTables.create(
                    new File(getStarRocksHome() + "/" + MOCKED_PARTITIONED_TRANSFORMS_DB_NAME + "/"
                            + tableName), tableName, schema, spec, 1);
        }

        @Override
        public List<String> getTransformTablePartitionNames(String tblName) {
            return Lists.newArrayList("id=1/data=a/ts_year=2024", "id=2/data=a/ts_year=2024");
        }
    };

    // multi partition columns: id, data, bucket(ts, 10)
    public static final MIcebergTable T0_MULTI_BUCKET = new MIcebergTable() {
        @Override
        public String getTableName() {
            return "t0_multi_bucket";
        }

        @Override
        public TestTables.TestTable getTestTable(Schema schema) throws IOException {
            PartitionSpec spec =
                    PartitionSpec.builderFor(schema)
                            .identity("id")
                            .identity("data")
                            .bucket("ts", 10)
                            .build();
            String tableName = getTableName();
            return TestTables.create(
                    new File(getStarRocksHome() + "/" + MOCKED_PARTITIONED_TRANSFORMS_DB_NAME + "/"
                            + tableName), tableName, schema, spec, 1);
        }

        @Override
        public List<String> getTransformTablePartitionNames(String tblName) {
            return Lists.newArrayList("id=1/data=a/ts_bucket=0", "id=2/data=a/ts_bucket=1");
        }
    };

    // multi partition columns: id, data, day(ts)
    public static final MIcebergTable T0_MULTI_DAY_TZ = new MIcebergTable() {
        @Override
        public String getTableName() {
            return "t0_multi_day_tz";
        }

        @Override
        public TestTables.TestTable getTestTable(Schema schema) throws IOException {
            PartitionSpec spec =
                    PartitionSpec.builderFor(schema)
                            .identity("id")
                            .identity("data")
                            .day("ts")
                            .build();
            String tableName = getTableName();
            return TestTables.create(
                    new File(getStarRocksHome() + "/" + MOCKED_PARTITIONED_TRANSFORMS_DB_NAME + "/"
                            + tableName), tableName, schema, spec, 1);
        }

        @Override
        public List<String> getTransformTablePartitionNames(String tblName) {
            return Lists.newArrayList("id=1/data=a/ts_day=2022-01-01", "id=2/data=a/ts_day=2022-01-02");
        }
    };

    public static Map<String, MIcebergTable> MOCKED_ICEBERG_TABLES = ImmutableMap.<String, MIcebergTable>builder()
            .put(T0_MULTI_HOUR.getTableName(), T0_MULTI_HOUR)
            .put(T0_MULTI_DAY.getTableName(), T0_MULTI_DAY)
            .put(T0_MULTI_MONTH.getTableName(), T0_MULTI_MONTH)
            .put(T0_MULTI_YEAR.getTableName(), T0_MULTI_YEAR)
            .put(T0_MULTI_BUCKET.getTableName(), T0_MULTI_BUCKET)
            .put(T0_MULTI_DAY_TZ.getTableName(), T0_MULTI_DAY_TZ)
            .build();
}
