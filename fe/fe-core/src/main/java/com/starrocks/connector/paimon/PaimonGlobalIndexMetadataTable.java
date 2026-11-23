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

package com.starrocks.connector.paimon;

import com.starrocks.catalog.PaimonTable;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.metadata.MetadataTable;
import com.starrocks.connector.metadata.MetadataTableType;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarbinaryType;

import java.util.List;

/** Query-scoped metadata relation. It never enters the persistent catalog. */
public final class PaimonGlobalIndexMetadataTable extends MetadataTable {
    public static final String SUFFIX = "$paimon_global_index";
    private final PaimonTable origin;

    public PaimonGlobalIndexMetadataTable(PaimonTable origin) {
        super(origin.getCatalogName(), ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asLong(),
                origin.getCatalogTableName() + SUFFIX, TableType.METADATA,
                builder().columns(PLACEHOLDER_COLUMNS).column("shard_id", IntegerType.BIGINT)
                        .column("index_result", VarbinaryType.VARBINARY).build(),
                origin.getCatalogDBName(), origin.getCatalogTableName(), MetadataTableType.PAIMON_GLOBAL_INDEX);
        this.origin = origin;
    }

    public PaimonTable getOrigin() {
        return origin;
    }

    @Override
    public boolean supportBuildPlan() {
        return true;
    }

    @Override
    public boolean isInternalOnly() {
        return true;
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        TTableDescriptor descriptor = origin.toThrift(partitions);
        descriptor.setId(getId());
        descriptor.setTableName(getName());
        descriptor.setNumCols(2);
        return descriptor;
    }
}
