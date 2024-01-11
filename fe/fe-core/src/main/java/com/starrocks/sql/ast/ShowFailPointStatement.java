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

import com.google.common.base.Joiner;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public class ShowFailPointStatement extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Name", ScalarType.createVarchar(256)))
                    .addColumn(new Column("TriggerMode", ScalarType.createVarchar(32)))
                    .addColumn(new Column("Times/Probability", ScalarType.createVarchar(16)))
                    .addColumn(new Column("Host", ScalarType.createVarchar(64)))
                    .build();

    private String pattern;
    private List<String> backends = null;

    public ShowFailPointStatement(String pattern, List<String> backends, NodePosition pos) {
        super(pos);
        this.pattern = pattern;
        this.backends = backends;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return null;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }

    public List<String> getBackends() {
        return backends;
    }

    public String getPattern() {
        return pattern;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder("SHOW FAILPOINTS");
        if (pattern != null) {
            sb.append(" LIKE '").append(pattern).append("'");
        }
        if (backends != null) {
            sb.append(" ON BACKEND '").append(Joiner.on(",").join(backends)).append("'");
        }
        return sb.toString();
    }
}
