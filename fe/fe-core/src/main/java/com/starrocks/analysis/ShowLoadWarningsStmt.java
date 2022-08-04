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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.UserException;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.AstVisitor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URL;

// SHOW LOAD WARNINGS statement used to get error detail of src data.
public class ShowLoadWarningsStmt extends ShowStmt {
    private static final Logger LOG = LogManager.getLogger(ShowLoadWarningsStmt.class);

    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("JobId", ScalarType.createVarchar(15)))
                    .addColumn(new Column("Label", ScalarType.createVarchar(15)))
                    .addColumn(new Column("ErrorMsgDetail", ScalarType.createVarchar(100)))
                    .build();

    private String dbName;
    private String rawUrl;
    private URL url;
    private Expr whereClause;
    private LimitElement limitElement;

    private String label;
    private long jobId;

    public ShowLoadWarningsStmt(String db, String url, Expr labelExpr,
                                LimitElement limitElement) {
        this.dbName = db;
        this.rawUrl = url;
        this.whereClause = labelExpr;
        this.limitElement = limitElement;

        this.label = null;
        this.jobId = 0;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public Expr getWhereClause() {
        return whereClause;
    }

    public LimitElement getLimitElement() {
        return limitElement;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public long getJobId() {
        return jobId;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public long getLimitNum() {
        if (limitElement != null && limitElement.hasLimit()) {
            return limitElement.getLimit();
        }
        return -1L;
    }

    public boolean isFindByLabel() {
        return label != null;
    }

    public boolean isFindByJobId() {
        return jobId != 0;
    }

    public String getRawUrl() {
        return rawUrl;
    }

    public URL getURL() {
        return url;
    }

    public void setUrl(URL url) {
        this.url = url;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowLoadWarningsStmt(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
}
