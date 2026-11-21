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

package com.starrocks.alter;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.common.SchemaVersionAndHash;
import com.starrocks.persist.image.ProtoLists;
import com.starrocks.persist.image.ProtoMaps;
import com.starrocks.persist.proto.ColumnListPB;
import com.starrocks.persist.proto.Int64ListPB;
import com.starrocks.persist.proto.Int64MapPB;
import com.starrocks.persist.proto.MaterializedIndexPB;
import com.starrocks.persist.proto.PartitionIndexPB;
import com.starrocks.persist.proto.PartitionIndexTabletsPB;
import com.starrocks.persist.proto.SchemaVersionAndHashPB;
import com.starrocks.persist.proto.StringListPB;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The image v3 forms of the collection shapes the alter jobs share: Guava tables keyed by physical partition and
 * index meta, and maps whose values are collections or persisted objects. Map entries with a null value are dropped,
 * as GSON drops them.
 */
final class AlterJobProtos {
    private AlterJobProtos() {
    }

    static List<PartitionIndexPB> toPartitionIndexProtos(Table<Long, Long, MaterializedIndex> table) {
        List<PartitionIndexPB> protos = new ArrayList<>(table.size());
        for (Table.Cell<Long, Long, MaterializedIndex> cell : table.cellSet()) {
            protos.add(PartitionIndexPB.newBuilder()
                    .setPhysicalPartitionId(cell.getRowKey())
                    .setIndexMetaId(cell.getColumnKey())
                    .setIndex(cell.getValue().toProto())
                    .build());
        }
        return protos;
    }

    static Table<Long, Long, MaterializedIndex> partitionIndexesFromProto(List<PartitionIndexPB> protos) {
        Table<Long, Long, MaterializedIndex> table = HashBasedTable.create();
        for (PartitionIndexPB cell : protos) {
            table.put(cell.getPhysicalPartitionId(), cell.getIndexMetaId(), MaterializedIndex.fromProto(cell.getIndex()));
        }
        return table;
    }

    static List<PartitionIndexTabletsPB> toPartitionIndexTabletsProtos(Table<Long, Long, Map<Long, Long>> table) {
        List<PartitionIndexTabletsPB> protos = new ArrayList<>(table.size());
        for (Table.Cell<Long, Long, Map<Long, Long>> cell : table.cellSet()) {
            protos.add(PartitionIndexTabletsPB.newBuilder()
                    .setPhysicalPartitionId(cell.getRowKey())
                    .setIndexMetaId(cell.getColumnKey())
                    .putAllTabletIds(ProtoMaps.withoutNullValues(cell.getValue()))
                    .build());
        }
        return protos;
    }

    static Table<Long, Long, Map<Long, Long>> partitionIndexTabletsFromProto(List<PartitionIndexTabletsPB> protos) {
        Table<Long, Long, Map<Long, Long>> table = HashBasedTable.create();
        for (PartitionIndexTabletsPB cell : protos) {
            table.put(cell.getPhysicalPartitionId(), cell.getIndexMetaId(), new HashMap<>(cell.getTabletIdsMap()));
        }
        return table;
    }

    static Int64MapPB int64Map(Map<Long, Long> map) {
        return Int64MapPB.newBuilder().putAllValues(ProtoMaps.withoutNullValues(map)).build();
    }

    static Map<Long, Long> int64MapFromProto(Int64MapPB pb) {
        return new HashMap<>(pb.getValuesMap());
    }

    static Map<Long, Int64MapPB> toInt64MapProtos(Map<Long, Map<Long, Long>> map) {
        Map<Long, Int64MapPB> protos = new HashMap<>();
        for (Map.Entry<Long, Map<Long, Long>> entry : map.entrySet()) {
            if (entry.getValue() != null) {
                protos.put(entry.getKey(), int64Map(entry.getValue()));
            }
        }
        return protos;
    }

    static Map<Long, Map<Long, Long>> int64MapsFromProto(Map<Long, Int64MapPB> protos) {
        Map<Long, Map<Long, Long>> map = new HashMap<>();
        for (Map.Entry<Long, Int64MapPB> entry : protos.entrySet()) {
            map.put(entry.getKey(), int64MapFromProto(entry.getValue()));
        }
        return map;
    }

    static Map<Long, Int64ListPB> toInt64ListProtos(Map<Long, List<Long>> map) {
        Map<Long, Int64ListPB> protos = new HashMap<>();
        for (Map.Entry<Long, List<Long>> entry : map.entrySet()) {
            if (entry.getValue() != null) {
                protos.put(entry.getKey(), ProtoLists.int64List(entry.getValue()));
            }
        }
        return protos;
    }

    static Map<Long, List<Long>> int64ListsFromProto(Map<Long, Int64ListPB> protos) {
        Map<Long, List<Long>> map = new HashMap<>();
        for (Map.Entry<Long, Int64ListPB> entry : protos.entrySet()) {
            map.put(entry.getKey(), ProtoLists.toLongs(entry.getValue()));
        }
        return map;
    }

    static Map<Long, MaterializedIndexPB> toMaterializedIndexProtos(Map<Long, MaterializedIndex> map) {
        Map<Long, MaterializedIndexPB> protos = new HashMap<>();
        for (Map.Entry<Long, MaterializedIndex> entry : map.entrySet()) {
            if (entry.getValue() != null) {
                protos.put(entry.getKey(), entry.getValue().toProto());
            }
        }
        return protos;
    }

    static Map<Long, MaterializedIndex> materializedIndexesFromProto(Map<Long, MaterializedIndexPB> protos) {
        Map<Long, MaterializedIndex> map = new HashMap<>();
        for (Map.Entry<Long, MaterializedIndexPB> entry : protos.entrySet()) {
            map.put(entry.getKey(), MaterializedIndex.fromProto(entry.getValue()));
        }
        return map;
    }

    static Map<Long, ColumnListPB> toColumnListProtos(Map<Long, List<Column>> map) {
        Map<Long, ColumnListPB> protos = new HashMap<>();
        for (Map.Entry<Long, List<Column>> entry : map.entrySet()) {
            if (entry.getValue() != null) {
                protos.put(entry.getKey(), Column.toListProto(entry.getValue()));
            }
        }
        return protos;
    }

    static Map<Long, List<Column>> columnListsFromProto(Map<Long, ColumnListPB> protos) {
        Map<Long, List<Column>> map = new HashMap<>();
        for (Map.Entry<Long, ColumnListPB> entry : protos.entrySet()) {
            map.put(entry.getKey(), Column.fromListProto(entry.getValue()));
        }
        return map;
    }

    static Map<Long, SchemaVersionAndHashPB> toSchemaVersionAndHashProtos(Map<Long, SchemaVersionAndHash> map) {
        Map<Long, SchemaVersionAndHashPB> protos = new HashMap<>();
        for (Map.Entry<Long, SchemaVersionAndHash> entry : map.entrySet()) {
            if (entry.getValue() != null) {
                protos.put(entry.getKey(), entry.getValue().toProto());
            }
        }
        return protos;
    }

    static Map<Long, SchemaVersionAndHash> schemaVersionAndHashesFromProto(Map<Long, SchemaVersionAndHashPB> protos) {
        Map<Long, SchemaVersionAndHash> map = new HashMap<>();
        for (Map.Entry<Long, SchemaVersionAndHashPB> entry : protos.entrySet()) {
            map.put(entry.getKey(), SchemaVersionAndHash.fromProto(entry.getValue()));
        }
        return map;
    }

    /** Java shorts travel as int32. */
    static Map<Long, Integer> toInt32Values(Map<Long, Short> map) {
        Map<Long, Integer> values = new HashMap<>();
        for (Map.Entry<Long, Short> entry : map.entrySet()) {
            if (entry.getValue() != null) {
                values.put(entry.getKey(), entry.getValue().intValue());
            }
        }
        return values;
    }

    static Map<Long, Short> shortValuesFromProto(Map<Long, Integer> values) {
        Map<Long, Short> map = new HashMap<>();
        for (Map.Entry<Long, Integer> entry : values.entrySet()) {
            map.put(entry.getKey(), entry.getValue().shortValue());
        }
        return map;
    }

    /** Column ids travel by name, as GSON writes them. */
    static StringListPB columnIdList(Collection<ColumnId> ids) {
        StringListPB.Builder builder = StringListPB.newBuilder();
        for (ColumnId id : ids) {
            builder.addValues(id.getId());
        }
        return builder.build();
    }

    /** An insertion-ordered set, as GSON delivers it. */
    static Set<ColumnId> columnIdSetFromProto(StringListPB pb) {
        Set<ColumnId> ids = new LinkedHashSet<>();
        for (String id : pb.getValuesList()) {
            ids.add(ColumnId.create(id));
        }
        return ids;
    }
}
