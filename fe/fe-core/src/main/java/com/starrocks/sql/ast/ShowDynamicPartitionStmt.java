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

package com.starrocks.sql.ast;

import com.starrocks.sql.parser.NodePosition;

import static com.starrocks.common.util.Util.normalizeName;

public class ShowDynamicPartitionStmt extends ShowStmt {
    private String db;

    public ShowDynamicPartitionStmt(String db) {
        this(db, NodePosition.ZERO);
    }

    public ShowDynamicPartitionStmt(String db, NodePosition pos) {
        super(pos);
        this.db = normalizeName(db);
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = normalizeName(db);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitShowDynamicPartitionStatement(this, context);
    }
}