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
import com.starrocks.server.GlobalStateMgr;

import java.util.ArrayList;
import java.util.List;

public class TabletMetastore implements TabletMetastoreInterface {
    public List<PhysicalPartition> getAllPhysicalPartition(Partition partition) {
        return new ArrayList<>(partition.getSubPartitions());
    }

    @Override
    public PhysicalPartition getPhysicalPartition(Partition partition, Long physicalPartitionId) {
        return partition.getSubPartition(physicalPartitionId);
    }

    public void addPhysicalPartition(Partition partition, PhysicalPartition physicalPartition) {
        partition.addSubPartition(physicalPartition);
    }

    public void dropPhysicalPartition(Partition partition, Long physicalPartitionId) {
        partition.removeSubPartition(physicalPartitionId);
    }

    public List<MaterializedIndex> getMaterializedIndices(PhysicalPartition physicalPartition,
                                                          MaterializedIndex.IndexExtState indexExtState) {
        return physicalPartition.getMaterializedIndices(indexExtState);
    }

    public MaterializedIndex getMaterializedIndex(PhysicalPartition physicalPartition, Long mIndexId) {
        return physicalPartition.getIndex(mIndexId);
    }

    public void addMaterializedIndex(PhysicalPartition physicalPartition, MaterializedIndex materializedIndex) {
        physicalPartition.createRollupIndex(materializedIndex);
    }

    public void dropMaterializedIndex(PhysicalPartition physicalPartition, Long mIndexId) {
        physicalPartition.deleteRollupIndex(mIndexId);
    }

    public List<Tablet> getAllTablets(MaterializedIndex materializedIndex) {
        return materializedIndex.getTablets();
    }

    public List<Long> getAllTabletIDs(MaterializedIndex materializedIndex) {
        return materializedIndex.getTabletIdsInOrder();
    }

    public Tablet getTablet(MaterializedIndex materializedIndex, Long tabletId) {
        return materializedIndex.getTablet(tabletId);
    }

    public void addTablet(MaterializedIndex materializedIndex, Tablet tablet, TabletMeta tabletMeta) {
        materializedIndex.addTablet(tablet, tabletMeta);
    }

    public List<Replica> getAllReplicas(Tablet tablet) {
        return tablet.getAllReplicas();
    }

    public Replica getReplica(LocalTablet tablet, Long replicaId) {
        return tablet.getReplicaById(replicaId);
    }

    @Override
    public void addReplica(ReplicaPersistInfo replicaPersistInfo, Tablet tablet, Replica replica) {
        GlobalStateMgr.getCurrentState().getEditLog().logAddReplica(replicaPersistInfo);
    }
}
