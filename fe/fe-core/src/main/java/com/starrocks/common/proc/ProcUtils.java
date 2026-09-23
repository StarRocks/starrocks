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

import java.util.List;

public class ProcUtils {

    /**
     * Resolve a column name against a proc dir's title list, for the ORDER BY of a SHOW statement.
     *
     * Every proc dir that accepts ORDER BY needs this, and each one used to carry its own copy -
     * eight of them, in three spellings that differed only in how they got at the index. They all
     * answer the same question, and the answer must not drift: the index is handed straight to
     * ListComparator, so resolving against the wrong list, or resolving differently, sorts by
     * whatever column happens to sit at that position.
     *
     * The list is passed in rather than read from a field because it is per statement: the layouts
     * disagree on both names and positions, and PartitionsProcDir even builds its list at runtime.
     */
    public static int analyzeColumn(List<String> titleNames, String columnName) throws AnalysisException {
        for (int i = 0; i < titleNames.size(); ++i) {
            if (titleNames.get(i).equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        throw new AnalysisException("Title name[" + columnName + "] does not exist");
    }

    static long getDbId(String dbIdOrName) throws AnalysisException {
        try {
            return Long.parseLong(dbIdOrName);
        } catch (NumberFormatException e) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbIdOrName);
            if (db == null) {
                throw new AnalysisException("Unknown database id or name \"" + dbIdOrName + "\"");
            }
            return db.getId();
        }
    }

}
