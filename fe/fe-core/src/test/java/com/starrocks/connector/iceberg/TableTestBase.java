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

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.apache.iceberg.types.Types.NestedField.required;

public class TableTestBase {
    // Schema passed to create tables
    public static final Schema SCHEMA =
            new Schema(required(3, "id", Types.IntegerType.get()), required(4, "data", Types.StringType.get()));

    public static final Schema SCHEMA_B =
            new Schema(required(1, "k1", Types.IntegerType.get()), required(2, "k2", Types.IntegerType.get()));

    protected static final int BUCKETS_NUMBER = 16;

    // Partition spec used to create tables
    protected static final PartitionSpec SPEC =
            PartitionSpec.builderFor(SCHEMA).bucket("data", BUCKETS_NUMBER).build();
    protected static final PartitionSpec SPEC_B =
            PartitionSpec.builderFor(SCHEMA_B).identity("k2").build();
    protected static final PartitionSpec SPEC_B_1 = PartitionSpec.builderFor(SCHEMA_B).build();

    public static final DataFile FILE_A =
            DataFiles.builder(SPEC)
                    .withPath("/path/to/data-a.parquet")
                    .withFileSizeInBytes(10)
                    .withPartitionPath("data_bucket=0") // easy way to set partition data for now
                    .withRecordCount(2)
                    .build();

    public static final DataFile FILE_B_1 =
            DataFiles.builder(SPEC_B)
                    .withPath("/path/to/data-b1.parquet")
                    .withFileSizeInBytes(20)
                    .withPartitionPath("k2=2")
                    .withRecordCount(3)
                    .build();

    public static final DataFile FILE_B_2 =
            DataFiles.builder(SPEC_B)
                    .withPath("/path/to/data-b2.parquet")
                    .withFileSizeInBytes(20)
                    .withPartitionPath("k2=3")
                    .withRecordCount(4)
                    .build();

    public static final DataFile FILE_B_5 = DataFiles.builder(SPEC_B)
            .withPath("/path/to/data-b5.parquet")
            .withFileSizeInBytes(20)
            .withRecordCount(4)
            .build();

    static final FileIO FILE_IO = new TestTables.LocalFileIO();

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    protected File tableDir = null;
    protected File metadataDir = null;

    public TestTables.TestTable table = null;
    public TestTables.TestTable mockedNativeTableB = null;
    public TestTables.TestTable mockedNativeTableG = null;

    protected final int formatVersion = 1;

    @Before
    public void setupTable() throws Exception {
        this.tableDir = temp.newFolder();
        tableDir.delete(); // created by table create

        this.metadataDir = new File(tableDir, "metadata");
        this.table = create(SCHEMA, SPEC, "test");
        this.mockedNativeTableB = create(SCHEMA_B, SPEC_B, "tb");
        this.mockedNativeTableG = create(SCHEMA_B, SPEC_B_1, "tg");
    }

    @After
    public void cleanupTables() {
        TestTables.clearTables();
    }

    protected TestTables.TestTable create(Schema schema, PartitionSpec spec, String tableName) {
        return TestTables.create(tableDir, tableName, schema, spec, formatVersion);
    }

    ManifestFile writeManifest(DataFile... files) throws IOException {
        return writeManifest(null, files);
    }

    ManifestFile writeManifest(Long snapshotId, DataFile... files) throws IOException {
        File manifestFile = temp.newFile("input.m0.avro");
        Assert.assertTrue(manifestFile.delete());
        OutputFile outputFile = table.ops().io().newOutputFile(manifestFile.getCanonicalPath());

        ManifestWriter<DataFile> writer =
                ManifestFiles.write(formatVersion, table.spec(), outputFile, snapshotId);
        try {
            for (DataFile file : files) {
                writer.add(file);
            }
        } finally {
            writer.close();
        }

        return writer.toManifestFile();
    }
}
