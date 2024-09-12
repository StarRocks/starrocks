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
package com.starrocks.persist.catalog;

import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;

import java.util.List;
import java.util.Set;

public class TabletLog extends Tablet {
    public TabletLog(long id) {
        super(id);
    }

    @Override
    public long getDataSize(boolean singleReplica) {
        return 0;
    }

    @Override
    public long getRowCount(long version) {
        return 0;
    }

    @Override
    public Set<Long> getBackendIds() {
        return null;
    }

    @Override
    public List<Replica> getAllReplicas() {
        return null;
    }

    @Override
    public void getQueryableReplicas(
            List<Replica> allQuerableReplicas, List<Replica> localReplicas, long visibleVersion, long localBeId, int schemaHash) {

    }

    @Override
    public void getQueryableReplicas(List<Replica> allQuerableReplicas, List<Replica> localReplicas, long visibleVersion,
                                     long localBeId, int schemaHash, long warehouseId) {

    }
}
