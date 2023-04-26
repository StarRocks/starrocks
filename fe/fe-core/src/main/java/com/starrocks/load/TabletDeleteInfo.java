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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/TabletDeleteInfo.java

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

package com.starrocks.load;

import com.google.common.collect.Sets;
import com.starrocks.catalog.Replica;

import java.util.Set;

public class TabletDeleteInfo {
    private final long partitionId;
    private final long tabletId;
    private final Set<Replica> finishedReplicas;

    public TabletDeleteInfo(long partitionId, long tabletId) {
        this.partitionId = partitionId;
        this.tabletId = tabletId;
        this.finishedReplicas = Sets.newConcurrentHashSet();
    }

    public long getTabletId() {
        return tabletId;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public Set<Replica> getFinishedReplicas() {
        return finishedReplicas;
    }

    public boolean addFinishedReplica(Replica replica) {
        finishedReplicas.add(replica);
        return true;
    }
}
