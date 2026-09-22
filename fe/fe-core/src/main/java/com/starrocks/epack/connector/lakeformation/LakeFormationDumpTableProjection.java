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

package com.starrocks.epack.connector.lakeformation;

import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.dump.DumpTableProjection;

/**
 * Records a governed table in a query dump the way SHOW CREATE TABLE prints it.
 *
 * The executable table keeps physical column names on purpose, so without this the name lists and the Glue
 * parameters - which carry the full schema under keys like hive.table.column.names - would reach the dump.
 */
public class LakeFormationDumpTableProjection extends DumpTableProjection {

    @Override
    public Table forDump(Table table) {
        return table instanceof LakeFormationHiveTable lfTable
                ? LakeFormationDdlProjection.projectForDisplay(lfTable)
                : table;
    }
}
