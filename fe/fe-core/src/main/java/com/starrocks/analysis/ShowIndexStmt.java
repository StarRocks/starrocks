// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/ShowIndexStmt.java

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

package com.starrocks.analysis;

import com.google.common.base.Strings;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;

public class ShowIndexStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Table", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Non_unique", ScalarType.createVarchar(10)))
                    .addColumn(new Column("Key_name", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Seq_in_index", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Column_name", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Collation", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Cardinality", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Sub_part", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Packed", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Null", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Index_type", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Comment", ScalarType.createVarchar(80)))
                    .build();
    private String dbName;
    private TableName tableName;

    public ShowIndexStmt(String dbName, TableName tableName) {
        this.dbName = dbName;
        this.tableName = tableName;
    }

    public void init() {
        if (!Strings.isNullOrEmpty(dbName)) {
            tableName.setDb(dbName);
        }
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);

        if (!Strings.isNullOrEmpty(dbName)) {
            // if user specify the `from db`, overwrite the db in tableName with this db.
            // for example:
            //      show index from db1.tbl1 from db2;
            // with be rewrote to:
            //      show index from db2.tbl1;
            // this act same as in MySQL
            tableName.setDb(dbName);
        }
        tableName.analyze(analyzer);

        if (!GlobalStateMgr.getCurrentState().getAuth()
                .checkTblPriv(ConnectContext.get(), tableName.getDb(), tableName.getTbl(),
                        PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, analyzer.getQualifiedUser(),
                    tableName.toString());
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder("SHOW INDEX FROM ");
        sb.append(tableName.toSql());
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    public String getDbName() {
        return tableName.getDb();
    }

    public TableName getTableName() {
        return tableName;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowIndexStmt(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
