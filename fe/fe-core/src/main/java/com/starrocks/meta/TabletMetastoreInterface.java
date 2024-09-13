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

package com.starrocks.meta;

import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.persist.ReplicaPersistInfo;

import java.util.List;

public interface TabletMetastoreInterface {
    List<PhysicalPartition> getAllPhysicalPartition(Partition partition);


    PhysicalPartition getPhysicalPartition(Partition partition, Long physicalPartitionId);

    void addPhysicalPartition(Partition partition, PhysicalPartition physicalPartition);

    void dropPhysicalPartition(Partition partition, Long physicalPartitionId);

    List<MaterializedIndex> getMaterializedIndices(PhysicalPartition physicalPartition,
                                                   MaterializedIndex.IndexExtState indexExtState);

    MaterializedIndex getMaterializedIndex(PhysicalPartition physicalPartition, Long mIndexId);

    void addMaterializedIndex(PhysicalPartition physicalPartition, MaterializedIndex materializedIndex);

    void dropMaterializedIndex(PhysicalPartition physicalPartition, Long mIndexId);

    List<Tablet> getAllTablets(MaterializedIndex materializedIndex);

    List<Long> getAllTabletIDs(MaterializedIndex materializedIndex);

    Tablet getTablet(MaterializedIndex materializedIndex, Long tabletId);

    void addTablet(MaterializedIndex materializedIndex, Tablet tablet, TabletMeta tabletMeta);

    List<Replica> getAllReplicas(Tablet tablet);

    Replica getReplica(LocalTablet tablet, Long replicaId);

    void addReplica(ReplicaPersistInfo replicaPersistInfo, Tablet tablet, Replica replica);
}
