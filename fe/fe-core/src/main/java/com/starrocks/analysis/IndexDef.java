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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/IndexDef.java

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
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.TreeSet;

public class IndexDef implements ParseNode {
    private String indexName;
    private List<String> columns;
    private IndexType indexType;
    private String comment;

    private final NodePosition pos;

    public IndexDef (String indexName, List<String> columns, IndexType indexType, String comment) {
        this(indexName, columns, indexType, comment, NodePosition.ZERO);
    }

    public IndexDef(String indexName, List<String> columns, IndexType indexType, String comment, NodePosition pos) {
        this.pos = pos;
        this.indexName = indexName;
        this.columns = columns;
        if (indexType == null) {
            this.indexType = IndexType.BITMAP;
        } else {
            this.indexType = indexType;
        }
        if (columns == null) {
            this.comment = "";
        } else {
            this.comment = comment;
        }
    }

    public void analyze() {
        if (indexType == IndexDef.IndexType.BITMAP) {
            if (columns == null || columns.size() != 1) {
                throw new SemanticException("bitmap index can only apply to a single column.");
            }
            if (Strings.isNullOrEmpty(indexName)) {
                throw new SemanticException("index name cannot be blank.");
            }
            if (indexName.length() > 64) {
                throw new SemanticException("index name too long, the index name length at most is 64.");
            }
            TreeSet<String> distinct = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            distinct.addAll(columns);
            if (columns.size() != distinct.size()) {
                throw new SemanticException("columns of index has duplicated.");
            }
        }
    }

    @Override
    public String toSql() {
        return toSql(null);
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    public String toSql(String tableName) {
        StringBuilder sb = new StringBuilder("INDEX ");
        sb.append(indexName);
        if (tableName != null && !tableName.isEmpty()) {
            sb.append(" ON ").append(tableName);
        }
        sb.append(" (");
        boolean first = true;
        for (String col : columns) {
            if (first) {
                first = false;
            } else {
                sb.append(",");
            }
            sb.append("`" + col + "`");
        }
        sb.append(")");
        if (indexType != null) {
            sb.append(" USING ").append(indexType.toString());
        }
        if (comment != null) {
            sb.append(" COMMENT '" + comment + "'");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    public String getIndexName() {
        return indexName;
    }

    public List<String> getColumns() {
        return columns;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public String getComment() {
        return comment;
    }

    // new planner framework use SemanticException instead of AnalysisException, this code will remove in future
    @Deprecated
    public void checkColumn(Column column, KeysType keysType) {
        if (indexType == IndexType.BITMAP) {
            String indexColName = column.getName();
            PrimitiveType colType = column.getPrimitiveType();
            if (!(colType.isDateType() ||
                    colType.isFixedPointType() || colType.isStringType() || colType == PrimitiveType.BOOLEAN)) {
                throw new SemanticException(colType + " is not supported in bitmap index. "
                        + "invalid column: " + indexColName);
            } else if ((keysType == KeysType.AGG_KEYS || keysType == KeysType.UNIQUE_KEYS) && !column.isKey()) {
                throw new SemanticException(
                        "BITMAP index only used in columns of DUP_KEYS/PRIMARY_KEYS table or key columns of"
                                + " UNIQUE_KEYS/AGG_KEYS table. invalid column: " + indexColName);
            }
        } else {
            throw new SemanticException("Unsupported index type: " + indexType);
        }
    }

    public enum IndexType {
        BITMAP,
    }
}
