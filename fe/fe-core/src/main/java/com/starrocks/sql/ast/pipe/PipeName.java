// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.sql.ast.pipe;

import com.starrocks.analysis.RedirectStatus;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.NodePosition;

import java.util.Objects;

public class PipeName extends StatementBase {

    private String dbName;
    private final String pipeName;

    public PipeName(NodePosition pos, String pipeName) {
        super(pos);
        this.dbName = null;
        this.pipeName = pipeName;
    }

    public PipeName(String dbName, String pipeName) {
        super(NodePosition.ZERO);
        this.dbName = dbName;
        this.pipeName = pipeName;
    }

    public PipeName(NodePosition pos, String dbName, String pipeName) {
        super(pos);
        this.dbName = dbName;
        this.pipeName = pipeName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getPipeName() {
        return pipeName;
    }

    @Override
    public String toSql() {
        return dbName + "." + pipeName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitPipeName(this, context);
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PipeName pipeName1 = (PipeName) o;
        return Objects.equals(dbName, pipeName1.dbName) && Objects.equals(pipeName, pipeName1.pipeName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbName, pipeName);
    }

    @Override
    public String toString() {
        if (dbName == null) {
            return pipeName;
        }
        return dbName + "." + pipeName;
    }
}
