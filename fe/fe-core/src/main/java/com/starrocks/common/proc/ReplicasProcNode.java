// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/proc/ReplicasProcNode.java

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

package com.starrocks.common.proc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Replica;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.system.Backend;

import java.util.Arrays;
import java.util.List;

/*
 * SHOW PROC /dbs/dbId/tableId/partitions/partitionId/indexId/tabletId
 * show replicas' detail info within a tablet
 */
public class ReplicasProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("ReplicaId").add("BackendId").add("Version").add("VersionHash")
            .add("LstSuccessVersion").add("LstSuccessVersionHash")
            .add("LstFailedVersion").add("LstFailedVersionHash")
            .add("LstFailedTime").add("SchemaHash").add("DataSize").add("RowCount").add("State")
            .add("IsBad").add("VersionCount").add("PathHash").add("MetaUrl").add("CompactionStatus")
            .build();

    private long tabletId;
    private List<Replica> replicas;

    public ReplicasProcNode(long tabletId, List<Replica> replicas) {
        this.tabletId = tabletId;
        this.replicas = replicas;
    }

    @Override
    public ProcResult fetchResult() {
        ImmutableMap<Long, Backend> backendMap = Catalog.getCurrentSystemInfo().getIdToBackend();

        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        for (Replica replica : replicas) {
            String metaUrl = String.format("http://%s:%d/api/meta/header/%d/%d",
                    backendMap.get(replica.getBackendId()).getHost(),
                    backendMap.get(replica.getBackendId()).getHttpPort(),
                    tabletId,
                    replica.getSchemaHash());

            String compactionUrl = String.format(
                    "http://%s:%d/api/compaction/show?tablet_id=%d&schema_hash=%d",
                    backendMap.get(replica.getBackendId()).getHost(),
                    backendMap.get(replica.getBackendId()).getHttpPort(),
                    tabletId,
                    replica.getSchemaHash());

            result.addRow(Arrays.asList(String.valueOf(replica.getId()),
                    String.valueOf(replica.getBackendId()),
                    String.valueOf(replica.getVersion()),
                    String.valueOf(replica.getVersionHash()),
                    String.valueOf(replica.getLastSuccessVersion()),
                    String.valueOf(replica.getLastSuccessVersionHash()),
                    String.valueOf(replica.getLastFailedVersion()),
                    String.valueOf(replica.getLastFailedVersionHash()),
                    TimeUtils.longToTimeString(replica.getLastFailedTimestamp()),
                    String.valueOf(replica.getSchemaHash()),
                    String.valueOf(replica.getDataSize()),
                    String.valueOf(replica.getRowCount()),
                    String.valueOf(replica.getState()),
                    String.valueOf(replica.isBad()),
                    String.valueOf(replica.getVersionCount()),
                    String.valueOf(replica.getPathHash()),
                    metaUrl,
                    compactionUrl));
        }
        return result;
    }
}

