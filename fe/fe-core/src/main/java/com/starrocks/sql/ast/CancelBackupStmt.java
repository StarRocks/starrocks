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


package com.starrocks.sql.ast;

import com.starrocks.sql.parser.NodePosition;

public class CancelBackupStmt extends CancelStmt {

    private String dbName;
    private final boolean isRestore;
    private final boolean isExternalCatalog;

    public CancelBackupStmt(String dbName, boolean isRestore) {
        this(dbName, isRestore, false, NodePosition.ZERO);
    }

    public CancelBackupStmt(String dbName, boolean isRestore, boolean isExternalCatalog) {
        this(dbName, isRestore, isExternalCatalog, NodePosition.ZERO);
    }

    public CancelBackupStmt(String dbName, boolean isRestore, boolean isExternalCatalog,
                            NodePosition pos) {
        super(pos);
        this.dbName = dbName;
        this.isRestore = isRestore;
        this.isExternalCatalog = isExternalCatalog;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public boolean isRestore() {
        return isRestore;
    }

    public boolean isExternalCatalog() {
        return isExternalCatalog;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCancelBackupStatement(this, context);
    }
}
