// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/CancelAlterTableStmt.java

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

import com.starrocks.analysis.CancelStmt;
import com.starrocks.analysis.TableName;
import com.starrocks.sql.ast.ShowAlterStmt.AlterType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.stream.Collectors;

/*
 * CANCEL ALTER COLUMN|ROLLUP FROM db_name.table_name
 */
public class CancelAlterTableStmt extends CancelStmt {

    private final AlterType alterType;

    private final TableName dbTableName;

    public AlterType getAlterType() {
        return alterType;
    }

    public String getDbName() {
        return dbTableName.getDb();
    }

    public void setDbName(String dbName) {
        this.dbTableName.setDb(dbName);
    }

    public TableName getDbTableName() {
        return this.dbTableName;
    }

    public String getTableName() {
        return dbTableName.getTbl();
    }

    private final List<Long> alterJobIdList;

    public CancelAlterTableStmt(AlterType alterType, TableName dbTableName) {
        this(alterType, dbTableName, null);
    }

    public CancelAlterTableStmt(AlterType alterType, TableName dbTableName, List<Long> alterJobIdList) {
        this.alterType = alterType;
        this.dbTableName = dbTableName;
        this.alterJobIdList = alterJobIdList;
    }

    public List<Long> getAlterJobIdList() {
        return alterJobIdList;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCancelAlterTableStatement(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }

    @Override
    public String toString() {
        return toSql();
    }

}
