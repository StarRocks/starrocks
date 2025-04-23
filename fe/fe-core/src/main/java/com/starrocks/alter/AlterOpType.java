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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/alter/AlterOpType.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.alter;

import com.starrocks.sql.ast.AddColumnClause;
import com.starrocks.sql.ast.AddColumnsClause;
import com.starrocks.sql.ast.AddFieldClause;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.AddRollupClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.CreateIndexClause;
import com.starrocks.sql.ast.DropColumnClause;
import com.starrocks.sql.ast.DropFieldClause;
import com.starrocks.sql.ast.DropIndexClause;
import com.starrocks.sql.ast.DropRollupClause;
import com.starrocks.sql.ast.ModifyColumnClause;
import com.starrocks.sql.ast.ModifyColumnCommentClause;
import com.starrocks.sql.ast.OptimizeClause;
import com.starrocks.sql.ast.ReorderColumnsClause;

public enum AlterOpType {
    // rollup
    ADD_ROLLUP,
    DROP_ROLLUP,
    // schema change
    SCHEMA_CHANGE,
    // partition
    ADD_PARTITION,
    // Optimize table
    OPTIMIZE,
    // ALTER AUTO_INCREMENT counter
    ALTER_AUTO_INCREMENT,
    // ALTER_OTHER must be the last one
    ALTER_OTHER,
    // INVALID_OP must be the last one
    INVALID_OP;

    // true means 2 operations have no conflict.
    public static final Boolean[][] COMPATIBILITY_MATRIX;

    static {
        COMPATIBILITY_MATRIX = new Boolean[INVALID_OP.ordinal() + 1][INVALID_OP.ordinal() + 1];
        for (int i = 0; i < INVALID_OP.ordinal(); i++) {
            for (int j = 0; j < INVALID_OP.ordinal(); j++) {
                COMPATIBILITY_MATRIX[i][j] = false;
            }
        }

        // rollup can be added or dropped in batch
        COMPATIBILITY_MATRIX[ADD_ROLLUP.ordinal()][ADD_ROLLUP.ordinal()] = true;
        COMPATIBILITY_MATRIX[DROP_ROLLUP.ordinal()][DROP_ROLLUP.ordinal()] = true;
        // schema change, such as add/modify/drop columns can be processed in batch
        COMPATIBILITY_MATRIX[SCHEMA_CHANGE.ordinal()][SCHEMA_CHANGE.ordinal()] = true;
    }

    public static boolean needCheckCapacity(AlterOpType alterOpType) {
        return alterOpType == ADD_ROLLUP
                || alterOpType == SCHEMA_CHANGE
                || alterOpType == ADD_PARTITION
                || alterOpType == OPTIMIZE;
    }

    public static AlterOpType getOpType(AlterClause alterClause) {
        if (alterClause instanceof AddColumnClause ||
                alterClause instanceof AddColumnsClause ||
                alterClause instanceof AddFieldClause ||
                alterClause instanceof CreateIndexClause ||
                alterClause instanceof DropColumnClause ||
                alterClause instanceof DropFieldClause ||
                alterClause instanceof DropIndexClause ||
                alterClause instanceof ModifyColumnClause ||
                alterClause instanceof ModifyColumnCommentClause ||
                alterClause instanceof ReorderColumnsClause) {
            return SCHEMA_CHANGE;
        } else if (alterClause instanceof AddPartitionClause) {
            return ADD_PARTITION;
        } else if (alterClause instanceof AddRollupClause) {
            return ADD_ROLLUP;
        } else if (alterClause instanceof DropRollupClause) {
            return DROP_ROLLUP;
        } else if (alterClause instanceof OptimizeClause) {
            return OPTIMIZE;
        } else if (alterClause instanceof AlterTableAutoIncrementClause) {
            return ALTER_AUTO_INCREMENT;
        }

        return ALTER_OTHER;
    }
}
