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

package com.starrocks.qe;

import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.UpdateStmt;

/**
 * Types of DML operation
 */
public enum DmlType {

    INSERT_INTO,
    INSERT_OVERWRITE,
    UPDATE,
    DELETE;

    public static DmlType fromStmt(DmlStmt stmt) {
        if (stmt instanceof InsertStmt) {
            InsertStmt insertStmt = (InsertStmt) stmt;
            if (insertStmt.isOverwrite()) {
                return INSERT_OVERWRITE;
            } else {
                return INSERT_INTO;
            }
        } else if (stmt instanceof UpdateStmt) {
            return UPDATE;
        } else if (stmt instanceof DeleteStmt) {
            return DELETE;
        } else {
            throw new UnsupportedOperationException("unsupported");
        }
    }

}
