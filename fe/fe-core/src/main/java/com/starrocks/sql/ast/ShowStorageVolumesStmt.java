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

import com.google.common.base.Strings;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;

public class ShowStorageVolumesStmt extends ShowStmt {
    private final String pattern;
    private static final String SV_COL = "Storage Volume";

    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column(SV_COL, ScalarType.createVarchar(256)))
                    .build();

    public ShowStorageVolumesStmt(String pattern, NodePosition pos) {
        super(pos);
        this.pattern = Strings.nullToEmpty(pattern);
    }


    public String getPattern() {
        return pattern;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowStorageVolumesStatement(this, context);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW STORAGE VOLUMES");
        if (!pattern.isEmpty()) {
            sb.append(" LIKE '").append(pattern).append("'");
        }
        return sb.toString();
    }
}
