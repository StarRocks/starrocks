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

package com.starrocks.common.proc;

import com.starrocks.catalog.Database;
import com.starrocks.common.AnalysisException;
import com.starrocks.server.GlobalStateMgr;

public class ProcUtils {

    static long getDbId(String dbIdOrName) throws AnalysisException {
        try {
            return Long.parseLong(dbIdOrName);
        } catch (NumberFormatException e) {
            Database db = GlobalStateMgr.getCurrentState().getDb(dbIdOrName);
            if (db == null) {
                throw new AnalysisException("Unknown database id or name \"" + dbIdOrName + "\"");
            }
            return db.getId();
        }
    }

}
