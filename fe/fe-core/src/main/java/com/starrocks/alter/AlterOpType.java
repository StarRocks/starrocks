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

<<<<<<< HEAD
=======
import com.starrocks.sql.ast.AddColumnClause;
import com.starrocks.sql.ast.AddColumnsClause;
import com.starrocks.sql.ast.AddFieldClause;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.AddRollupClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterTableAutoIncrementClause;
import com.starrocks.sql.ast.CreateIndexClause;
import com.starrocks.sql.ast.DropColumnClause;
import com.starrocks.sql.ast.DropFieldClause;
import com.starrocks.sql.ast.DropIndexClause;
import com.starrocks.sql.ast.DropRollupClause;
import com.starrocks.sql.ast.ModifyColumnClause;
import com.starrocks.sql.ast.ModifyColumnCommentClause;
import com.starrocks.sql.ast.OptimizeClause;
import com.starrocks.sql.ast.ReorderColumnsClause;

>>>>>>> 7cbfdee587 ([Enhancement] provide alter table xxx set auto_increment (#62767))
public enum AlterOpType {
    // rollup
    ADD_ROLLUP,
    DROP_ROLLUP,
    // schema change
    SCHEMA_CHANGE,
    // partition
    ADD_PARTITION,
    DROP_PARTITION,
    TRUNCATE_PARTITION,
    REPLACE_PARTITION,
    MODIFY_PARTITION,
    // rename
    RENAME,
    // table property
    MODIFY_TABLE_PROPERTY,
    MODIFY_TABLE_PROPERTY_SYNC, // Some operations are performed synchronously, so we distinguish them by suffix _SYNC
    // others operation, such as add/drop backend. currently we do not care about them
    ALTER_OTHER,
    SWAP,
    COMPACT,

    // comment
    ALTER_COMMENT,

    //Alter View
    ALTER_VIEW,
    REFRESH_SCHEMA,
    ALTER_MV_STATUS,

    // Optimize table
    OPTIMIZE,
<<<<<<< HEAD
    ALTER_BRANCH,
    ALTER_TAG,
    ALTER_TABLE_OPERATION,

    // dynamic tablet split
    SPLIT_TABLET,

    INVALID_OP; // INVALID_OP must be the last one
=======
    // ALTER AUTO_INCREMENT counter
    ALTER_AUTO_INCREMENT,
    // ALTER_OTHER must be the last one
    ALTER_OTHER,
    // INVALID_OP must be the last one
    INVALID_OP;
>>>>>>> 7cbfdee587 ([Enhancement] provide alter table xxx set auto_increment (#62767))

    // true means 2 operations have no conflict.
    public static Boolean[][] COMPATIBITLITY_MATRIX;

    static {
        COMPATIBITLITY_MATRIX = new Boolean[INVALID_OP.ordinal() + 1][INVALID_OP.ordinal() + 1];
        for (int i = 0; i < INVALID_OP.ordinal(); i++) {
            for (int j = 0; j < INVALID_OP.ordinal(); j++) {
                COMPATIBITLITY_MATRIX[i][j] = false;
            }
        }

        // rollup can be added or dropped in batch
        COMPATIBITLITY_MATRIX[ADD_ROLLUP.ordinal()][ADD_ROLLUP.ordinal()] = true;
        COMPATIBITLITY_MATRIX[DROP_ROLLUP.ordinal()][DROP_ROLLUP.ordinal()] = true;
        // schema change, such as add/modify/drop columns can be processed in batch
        COMPATIBITLITY_MATRIX[SCHEMA_CHANGE.ordinal()][SCHEMA_CHANGE.ordinal()] = true;
    }

    public boolean needCheckCapacity() {
        return this == ADD_ROLLUP || this == SCHEMA_CHANGE || this == ADD_PARTITION || this == OPTIMIZE;
    }

<<<<<<< HEAD
=======
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
>>>>>>> 7cbfdee587 ([Enhancement] provide alter table xxx set auto_increment (#62767))
}
