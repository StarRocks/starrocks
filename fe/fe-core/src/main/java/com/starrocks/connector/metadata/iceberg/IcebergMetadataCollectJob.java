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

package com.starrocks.connector.metadata.iceberg;

import com.starrocks.connector.metadata.MetadataCollectJob;
import com.starrocks.thrift.TResultSinkType;
import io.trino.hive.$internal.com.google.common.base.Strings;
import org.apache.velocity.VelocityContext;

public class IcebergMetadataCollectJob extends MetadataCollectJob {
    private static final String ICEBERG_METADATA_TEMPLATE = "SELECT content" + // INTEGER
            ", file_path" + // VARCHAR
            ", file_format" + // VARCHAR
            ", spec_id" + // INTEGER
            ", partition_data" + // BINARY
            ", record_count" + // BIGINT
            ", file_size_in_bytes" + // BIGINT
            ", split_offsets" + // ARRAY<BIGINT>
            ", sort_id" + // INTEGER
            ", equality_ids" + // ARRAY<INTEGER>
            ", file_sequence_number" + // BIGINT
            ", data_sequence_number " + // BIGINT
            ", column_stats " + // BINARY
            ", key_metadata " + // BINARY
            "FROM `$catalogName`.`$dbName`.`$tableName$logical_iceberg_metadata` " +
            "FOR VERSION AS OF $snapshotId " +
            "WHERE $predicate'";

    private final long snapshotId;
    private final String predicate;

    public IcebergMetadataCollectJob(String catalogName,
                                     String dbName,
                                     String tableName,
                                     TResultSinkType sinkType,
                                     long snapshotId,
                                     String predicate) {
        super(catalogName, dbName, tableName, sinkType);
        this.snapshotId = snapshotId;
        this.predicate = predicate;
    }

    public String buildCollectMetadataSQL() {
        StringBuilder builder = new StringBuilder();
        VelocityContext context = new VelocityContext();

        context.put("catalogName", getCatalogName());
        context.put("dbName", getDbName());
        context.put("tableName", getTableName());
        context.put("snapshotId", snapshotId);
        if (Strings.isNullOrEmpty(predicate)) {
            context.put("predicate", "1=1");
        } else {
            context.put("predicate", "predicate = '" + predicate + "'");
        }

        builder.append(build(context, ICEBERG_METADATA_TEMPLATE));
        return builder.toString();
    }
}
