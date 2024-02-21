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

package com.starrocks.warehouse;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.common.proc.ProcDirInterface;
import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.server.WarehouseManager;

import java.util.List;

public class WarehouseProcDir implements ProcDirInterface {
    public static final ImmutableList<String> WAREHOUSE_PROC_NODE_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Id")
            .add("Warehouse")
            .add("State")
            .add("ClusterCount")
            .build();

    private final WarehouseManager warehouseManager;

    public WarehouseProcDir(WarehouseManager manager) {
        this.warehouseManager = manager;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return true;
    }

    @Override
    public ProcNodeInterface lookup(String idOrName) throws AnalysisException {
        if (Strings.isNullOrEmpty(idOrName)) {
            throw new AnalysisException("warehouse id or name is null or empty");
        }
        Warehouse warehouse;
        try {
            warehouse = warehouseManager.getWarehouse(Long.parseLong(idOrName));
        } catch (NumberFormatException e) {
            warehouse = warehouseManager.getWarehouse(idOrName);
        }
        if (warehouse == null) {
            throw new AnalysisException("Unknown warehouse id or name \"" + idOrName + "\"");
        }
        return new WarehouseClusterProcNode(warehouse);
    }

    @Override
    public ProcResult fetchResult() {
        BaseProcResult result = new BaseProcResult();
        result.setNames(WAREHOUSE_PROC_NODE_TITLE_NAMES);
        List<Long> warehouseIds = warehouseManager.getWarehouseIds();
        warehouseIds.forEach(x -> {
            Warehouse wh = warehouseManager.getWarehouse(x);
            if (wh != null) {
                wh.getProcNodeData(result);
            }
        });
        return result;
    }
}
