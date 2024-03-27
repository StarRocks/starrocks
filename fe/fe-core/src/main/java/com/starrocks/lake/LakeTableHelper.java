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

package com.starrocks.lake;

import com.google.common.base.Preconditions;
import com.starrocks.alter.AlterJobV2Builder;
import com.starrocks.alter.LakeTableAlterJobV2Builder;
import com.starrocks.catalog.OlapTable;
import com.starrocks.server.GlobalStateMgr;

public class LakeTableHelper {
    static boolean deleteTable(long dbId, OlapTable table, boolean replay) {
        Preconditions.checkState(table.isCloudNativeTableOrMaterializedView());
        table.removeTableBinds(replay);
        // LakeTable's data cleaning operation is a time-consuming operation that may need to be repeated
        // many times before it succeeds, so we chose to let the recycle bin perform this work for two purposes:
        // 1. to avoid blocking the user's session for too long
        // 2. to utilize the recycle bin's delete retry capability to perform retries.
        GlobalStateMgr.getCurrentState().getRecycleBin().recycleTable(dbId, table, false);
        return true;
    }

    static boolean deleteTableFromRecycleBin(long dbId, OlapTable table, boolean replay) {
        Preconditions.checkState(table.isCloudNativeTableOrMaterializedView());
        table.removeTableBinds(replay);
        if (replay) {
            table.removeTabletsFromInvertedIndex();
            return true;
        }
        LakeTableCleaner cleaner = new LakeTableCleaner(table);
        boolean succ = cleaner.cleanTable();
        if (succ) {
            table.removeTabletsFromInvertedIndex();
        }
        return succ;
    }


    static AlterJobV2Builder alterTable(OlapTable table) {
        Preconditions.checkState(table.isCloudNativeTableOrMaterializedView());
        return new LakeTableAlterJobV2Builder(table);
    }
}
