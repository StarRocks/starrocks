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

package org.apache.iceberg;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ManifestEntryIteratorTest {
    private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    // Format-version 4 manifests are Parquet files: reading them needs iceberg-parquet on the reader's classpath.
    @Test
    public void testReadParquetManifestOfFormatV4Table() throws Exception {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("mem", Map.of(CatalogProperties.WAREHOUSE_LOCATION, "mem://warehouse"));
        catalog.createNamespace(Namespace.of("db"));
        Table table = catalog.createTable(TableIdentifier.of("db", "t"), SCHEMA, PartitionSpec.unpartitioned(),
                Map.of(TableProperties.FORMAT_VERSION, "4"));
        DataFile dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(table.location() + "/data/a.parquet").withFileSizeInBytes(10).withRecordCount(3).build();
        table.newFastAppend().appendFile(dataFile).commit();

        List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
        Assertions.assertEquals(1, manifests.size());
        Assertions.assertEquals(FileFormat.PARQUET, FileFormat.fromFileName(manifests.get(0).path()));

        List<String> paths = new ArrayList<>();
        try (ManifestEntryIterator it = new ManifestEntryIterator(manifests.get(0), table.io(), table.specs())) {
            while (it.hasNext()) {
                it.next();
                paths.add(it.file().location());
                Assertions.assertEquals(3, it.file().recordCount());
            }
        }
        Assertions.assertEquals(List.of(dataFile.location()), paths);
    }
}
