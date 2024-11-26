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

package com.starrocks.catalog.system.information;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.sql.ast.ShowAnalyzeStatusStmt;
import com.starrocks.thrift.TSchemaTableType;

import java.util.List;

/**
 * | Id | Database| Table| Columns| Type| Schedule | Status  | StartTime| EndTime| Properties | Reason |
 */
public class AnalyzeStatusSystemTable extends SystemTable {

    private static final String NAME = "analyze_status";
    private static final List<Column> COLUMNS = ShowAnalyzeStatusStmt.META_DATA.getColumns();

    public AnalyzeStatusSystemTable() {
        super(SystemId.ANALYZE_STATUS, NAME, TableType.SCHEMA, COLUMNS, TSchemaTableType.SCH_ANALYZE_STATUS);
    }

    public static SystemTable create() {
        return new AnalyzeStatusSystemTable();
    }

}
