// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.format;

import com.starrocks.format.util.DataType;
import com.starrocks.proto.TabletSchema.ColumnPB;
import com.starrocks.proto.TabletSchema.KeysType;
import com.starrocks.proto.TabletSchema.TabletSchemaPB;
import com.starrocks.proto.Types;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

class StarRocksWriterTest extends BaseFormatTest {

    @Test
    public void testWrite(@TempDir Path tempDir) throws Exception {
        String tabletRootPath = tempDir.toAbsolutePath().toString();

        final long tabletId = RandomUtils.nextLong(0, Integer.MAX_VALUE);
        TabletSchemaPB.Builder schemaBuilder = TabletSchemaPB.newBuilder()
                .setId(tabletId)
                .setKeysType(KeysType.DUP_KEYS)
                .setCompressionType(Types.CompressionTypePB.LZ4_FRAME);

        int colUniqueId = 0;
        // add key column
        schemaBuilder.addColumn(ColumnPB.newBuilder()
                .setUniqueId(colUniqueId++)
                .setName("id")
                .setType(DataType.BIGINT.getLiteral())
                .setIsKey(true)
                .setIsNullable(false)
                .setLength(8)
                .setIndexLength(8)
                .build());

        ColumnType[] columnTypes = new ColumnType[] {
                new ColumnType(DataType.BOOLEAN, 1),
                new ColumnType(DataType.TINYINT, 1),
                new ColumnType(DataType.SMALLINT, 2),
                new ColumnType(DataType.INT, 4),
                new ColumnType(DataType.BIGINT, 8),
                new ColumnType(DataType.LARGEINT, 16),
                new ColumnType(DataType.FLOAT, 4),
                new ColumnType(DataType.DOUBLE, 8),
                new ColumnType(DataType.DECIMAL64, 8),
                new ColumnType(DataType.DATE, 16),
                new ColumnType(DataType.DATETIME, 16),
                new ColumnType(DataType.CHAR, 32),
                new ColumnType(DataType.VARCHAR, 1024),
                new ColumnType(DataType.JSON, 16)
        };

        for (ColumnType columnType : columnTypes) {
            ColumnPB.Builder columnBuilder = ColumnPB.newBuilder()
                    .setUniqueId(colUniqueId++)
                    .setName("c_" + columnType.getDataType().getLiteral().toLowerCase())
                    .setType(columnType.getDataType().getLiteral())
                    .setIsKey(false)
                    .setIsNullable(true)
                    .setLength(columnType.getLength())
                    .setIndexLength(columnType.getLength())
                    .setAggregation("NONE");
            if (DataType.DECIMAL32.equals(columnType.getDataType())) {
                columnBuilder.setPrecision(9);
                columnBuilder.setFrac(2);
            } else if (DataType.DECIMAL64.equals(columnType.getDataType())) {
                columnBuilder.setPrecision(18);
                columnBuilder.setFrac(3);
            } else if (DataType.DECIMAL128.equals(columnType.getDataType())) {
                columnBuilder.setPrecision(38);
                columnBuilder.setFrac(4);
            }
            schemaBuilder.addColumn(columnBuilder.build());
        }
        TabletSchemaPB tabletSchema = schemaBuilder
                .setNextColumnUniqueId(colUniqueId)
                // sort key index always the key column index
                .addSortKeyIdxes(0)
                // short key size is less than sort keys
                .setNumShortKeyColumns(1)
                .setNumRowsPerRowBlock(1024)
                .build();

        long visibleVersion = writeTabletMeta(tabletRootPath, 1, tabletSchema);

        final long txnId = RandomUtils.nextLong(1, Integer.MAX_VALUE);
        Schema schema = toArrowSchema(tabletSchema);
        try (StarRocksWriter writer = new StarRocksWriter(
                tabletId,
                tabletRootPath,
                txnId,
                schema,
                Config.newBuilder().build())) {
            writer.open();

            // write use chunk interface
            VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, writer.getAllocator());

            fillRowData(vsr, 0, 5);
            writer.write(vsr);
            vsr.close();

            writer.flush();
            writer.finish();
        }

        try (StarRocksReader reader = new StarRocksReader(
                tabletId,
                tabletRootPath,
                visibleVersion,
                schema,
                schema,
                Config.newBuilder().build())) {
            reader.open();
            while (reader.hasNext()) {
                VectorSchemaRoot root = reader.next();
                System.out.println(root.contentToTSVString());
            }
        }

    }

}