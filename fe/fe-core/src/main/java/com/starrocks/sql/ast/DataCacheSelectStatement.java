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

import com.google.common.base.Preconditions;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.qe.OriginStatement;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

public class DataCacheSelectStatement extends DdlStmt {

    private final InsertStmt insertStmt;

    private final Map<String, String> properties;

    // =================================================================================
    // Below properties will set after DataCacheAnalyzer analyze properties
    private boolean isVerbose = false;
    // real catalog of cache select table
    private String catalog = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
    private long ttlSeconds = 0;
    private int priority = 0;
    // =================================================================================

    public DataCacheSelectStatement(InsertStmt insertStmt, Map<String, String> properties, NodePosition pos) {
        super(pos);
        this.insertStmt = insertStmt;
        this.properties = properties;
        Preconditions.checkNotNull(properties, "properties can't be null");
        insertStmt.setOrigStmt(new OriginStatement("CACHE " + AstToSQLBuilder.toSQL(insertStmt.getQueryStatement())));
    }

    public InsertStmt getInsertStmt() {
        return insertStmt;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setVerbose(boolean verbose) {
        isVerbose = verbose;
    }

    public boolean isVerbose() {
        return isVerbose;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public String getCatalog() {
        return this.catalog;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public int getPriority() {
        return priority;
    }

    public void setTTLSeconds(long ttlSeconds) {
        this.ttlSeconds = ttlSeconds;
    }

    public long getTTLSeconds() {
        return ttlSeconds;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDataCacheSelectStatement(this, context);
    }
}
