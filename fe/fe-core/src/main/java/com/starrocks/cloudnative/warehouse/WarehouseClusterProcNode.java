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

package com.starrocks.cloudnative.warehouse;

import com.starrocks.common.exception.AnalysisException;
import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.common.proc.ProcResult;

public class WarehouseClusterProcNode implements ProcNodeInterface {
    private final Warehouse warehouse;

    public WarehouseClusterProcNode(Warehouse wh) {
        this.warehouse = wh;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        return warehouse.getClusterProcData();
    }
}
