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

package com.starrocks.http.rest.v2.vo;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Tablet;
import com.starrocks.lake.LakeTablet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;

import java.util.Set;

public class TabletView {

    @SerializedName("id")
    private Long id;

    @SerializedName("primaryComputeNodeId")
    private Long primaryComputeNodeId;

    @SerializedName("backendIds")
    private Set<Long> backendIds;

    public TabletView() {
    }

    /**
     * Create from {@link Tablet}
     */
    public static TabletView createFrom(Tablet tablet) {
        TabletView tvo = new TabletView();
        tvo.setId(tablet.getId());
        tvo.setBackendIds(tablet.getBackendIds());

        if (tablet instanceof LakeTablet) {
            LakeTablet lakeTablet = (LakeTablet) tablet;
            Long computeNodeId = GlobalStateMgr.getCurrentState().getWarehouseMgr()
                    .getComputeNodeId(WarehouseManager.DEFAULT_WAREHOUSE_ID, lakeTablet);
            tvo.setPrimaryComputeNodeId(computeNodeId);
        }

        return tvo;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getPrimaryComputeNodeId() {
        return primaryComputeNodeId;
    }

    public void setPrimaryComputeNodeId(Long primaryComputeNodeId) {
        this.primaryComputeNodeId = primaryComputeNodeId;
    }

    public Set<Long> getBackendIds() {
        return backendIds;
    }

    public void setBackendIds(Set<Long> backendIds) {
        this.backendIds = backendIds;
    }
}
