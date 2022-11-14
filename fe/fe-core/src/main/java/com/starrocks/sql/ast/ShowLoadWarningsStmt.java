// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LimitElement;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;
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
    private final String rawUrl;
    private URL url;
    private final Expr whereClause;
    private final LimitElement limitElement;

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
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowLoadWarningsStatement(this, context);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
}
