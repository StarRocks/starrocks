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

package com.starrocks.backup;

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.persist.proto.BackupTablePB;

import java.io.IOException;
import java.io.UncheckedIOException;

/** Complete tables owned by backup jobs; these must not be installed in the live metastore. */
final class BackupCatalogProtos {
    private BackupCatalogProtos() {
    }

    static BackupTablePB toProto(Table table) {
        BackupTablePB.Builder builder = BackupTablePB.newBuilder();
        try {
            builder.setTable(table.toProto());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (table instanceof OlapTable) {
            OlapTable olap = (OlapTable) table;
            olap.getPartitions().forEach(partition -> builder.addPartitions(partition.toProto()));
            olap.getTempPartitions().forEach(partition -> builder.addTempPartitions(partition.toProto()));
        }
        return builder.build();
    }

    static Table fromProto(BackupTablePB pb) {
        Table table;
        try {
            table = Table.fromProto(pb.getTable());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (table instanceof OlapTable) {
            OlapTable olap = (OlapTable) table;
            pb.getPartitionsList().forEach(partition -> olap.addPartition(Partition.fromProto(partition)));
            pb.getTempPartitionsList().forEach(partition -> olap.addTempPartition(Partition.fromProto(partition)));
        } else if (pb.getPartitionsCount() != 0 || pb.getTempPartitionsCount() != 0) {
            throw new IllegalArgumentException("partitions on backup table " + table.getType());
        }
        return table;
    }
}
