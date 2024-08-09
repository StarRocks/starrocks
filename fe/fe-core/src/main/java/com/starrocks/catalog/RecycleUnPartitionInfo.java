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

package com.starrocks.catalog;

import com.starrocks.common.DdlException;
import com.starrocks.lake.DataCacheInfo;

// This class is simply used as the Interface to trigger the background task
// in CatalogRecycleBin
public class RecycleUnPartitionInfo extends RecyclePartitionInfoV2 {
    public RecycleUnPartitionInfo(long dbId, long tableId, Partition partition, DataProperty dataProperty,
                                  short replicationNum, boolean isInMemory,
                                  DataCacheInfo dataCacheInfo) {
        super(dbId, tableId, partition, dataProperty, replicationNum, isInMemory, dataCacheInfo);
        setRecoverable(false);
    }

    @Override
    public void recover(OlapTable table) throws DdlException {
        throw new DdlException("Does not support recover unpartitioned");
    }
}
