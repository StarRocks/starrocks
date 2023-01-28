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

package com.starrocks.load;

import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Type;
import com.starrocks.thrift.TParquetColumn;
import com.starrocks.thrift.TParquetColumnType;
import com.starrocks.thrift.TParquetCompressionType;
import com.starrocks.thrift.TParquetRepetitionType;
import com.starrocks.thrift.TParquetSchema;

import java.util.List;
import java.util.Map;

import static com.starrocks.sql.common.UnsupportedException.unsupportedException;

public class ParquetUtils {
    public static final Map<String, TParquetCompressionType> PARQUET_COMPRESSION_TYPE_MAP =
            Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
    public static final String PARQUET_COMPRESSION_TYPE = "compression_type";
    public static final String PARQUET_USE_DICT = "use_dictionary";
    public static final String PARQUET_MAX_ROW_GROUP_SIZE = "max_row_group_bytes";
    public static final long DEFAULT_MAX_PARQUET_ROW_GROUP_BYTES = 128 * 1024 * 1024; // 128MB

    static {
        PARQUET_COMPRESSION_TYPE_MAP.put("snappy", TParquetCompressionType.SNAPPY);
        PARQUET_COMPRESSION_TYPE_MAP.put("gzip", TParquetCompressionType.GZIP);
        PARQUET_COMPRESSION_TYPE_MAP.put("brotli", TParquetCompressionType.BROTLI);
        PARQUET_COMPRESSION_TYPE_MAP.put("zstd", TParquetCompressionType.ZSTD);
        PARQUET_COMPRESSION_TYPE_MAP.put("lz4", TParquetCompressionType.LZ4);
        PARQUET_COMPRESSION_TYPE_MAP.put("lzo", TParquetCompressionType.LZO);
        PARQUET_COMPRESSION_TYPE_MAP.put("bz2", TParquetCompressionType.BZ2);
        PARQUET_COMPRESSION_TYPE_MAP.put("default", TParquetCompressionType.UNCOMPRESSED);
    }

    public static TParquetSchema buildParquetSchema(List<Expr> outputExprs, List<String> columnNames) {
        if (!columnNames.isEmpty() && outputExprs.size() != columnNames.size()) {
            throw new IllegalArgumentException(String.format("output expr size %d isn't equal to column names size %d",
                    outputExprs.size(), columnNames.size()));
        }

        TParquetSchema parquetSchema = new TParquetSchema();
        for (int i = 0; i < outputExprs.size(); i++) {
            TParquetColumn parquetColumn = new TParquetColumn();
            Expr expr = outputExprs.get(i);
            parquetColumn.setName(columnNames.get(i));
            parquetColumn.setRepetition_type(buildParquetRepetitionType(expr));
            parquetColumn.setType(buildParquetColumnType(expr.getType()));

            parquetSchema.addToColumns(parquetColumn);
        }
        return parquetSchema;
    }

    public static TParquetRepetitionType buildParquetRepetitionType(Expr expr) {
        if (expr.isNullable()) {
            return TParquetRepetitionType.OPTIONAL;
        } else {
            return TParquetRepetitionType.REQUIRED;
        }
    }

    public static TParquetColumnType buildParquetColumnType(Type srType) {
        switch (srType.getPrimitiveType()) {
            case BOOLEAN:
                return TParquetColumnType.BOOLEAN;
            case TINYINT:
            case SMALLINT:
            case INT:
                return TParquetColumnType.INT32;
            case BIGINT:
            case DATE:
            case DATETIME:
                return TParquetColumnType.INT64;
            case FLOAT:
                return TParquetColumnType.FLOAT;
            case DOUBLE:
                return TParquetColumnType.DOUBLE;
            case CHAR:
            case VARCHAR:
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return TParquetColumnType.BYTE_ARRAY;

            default:
                throw unsupportedException(String.format("%s is not supported", srType.getPrimitiveType().toString()));
        }
    }

}
