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
import com.starrocks.analysis.BloomFilterIndexUtil;
import com.starrocks.analysis.InvertedIndexUtil;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.VectorIndexUtil;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.sql.parser.StarRocksParser.IndexTypeContext;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;

public class IndexDef implements ParseNode {

    private static final int MAX_INDEX_NAME_LENGTH = 64;

    private final String indexName;
    private final List<String> columns;
    private final IndexType indexType;
    private final String comment;
    private final Map<String, String> properties;

    private final NodePosition pos;

    public IndexDef(String indexName, List<String> columns, IndexType indexType, String comment) {
        this(indexName, columns, indexType, comment, Collections.emptyMap(), NodePosition.ZERO);
    }

    public IndexDef(String indexName, List<String> columns, IndexType indexType, String comment, Map<String, String> properties) {
        this(indexName, columns, indexType, comment, properties, NodePosition.ZERO);
    }

    public IndexDef(String indexName, List<String> columns, IndexType indexType, String comment, Map<String, String> properties,
            NodePosition pos) {
        this.pos = pos;
        this.indexName = indexName;
        this.columns = columns;
        this.indexType = Optional.ofNullable(indexType).orElse(IndexType.BITMAP);
        this.comment = Optional.ofNullable(comment).orElse(StringUtils.EMPTY);
        this.properties = Optional.ofNullable(properties).orElse(Collections.emptyMap());
    }

    public void analyze() {
        if (columns == null) {
            throw new SemanticException("Index can not accept null column.");
        }
        if (Strings.isNullOrEmpty(indexName)) {
            throw new SemanticException("index name cannot be blank.");
        }
        if (indexName.length() > MAX_INDEX_NAME_LENGTH) {
            throw new SemanticException("index name too long, the index name length at most is 64.");
        }
        TreeSet<String> distinct = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        distinct.addAll(columns);
        if (columns.size() != distinct.size()) {
            throw new SemanticException("columns of index has duplicated.");
        }

        // right now only support single column index
        if (columns.size() != 1) {
            throw new SemanticException(indexName + " index can only apply to a single column.");
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
            sb.append("`").append(col).append("`");
        }
        sb.append(")");
        if (indexType != null) {
            sb.append(" USING ").append(indexType);
        }
        if (properties != null && properties.size() > 0) {
            sb.append(" (");
            first = true;
            for (Map.Entry<String, String> e : properties.entrySet()) {
                if (first) {
                    first = false;
                } else {
                    sb.append(", ");
                }
                sb.append("\"").append(e.getKey()).append("\"=").append("\"").append(e.getValue()).append("\"");
            }
            sb.append(")");
        }
        if (comment != null) {
            sb.append(" COMMENT '").append(comment).append("'");
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

    public Map<String, String> getProperties() {
        return properties;
    }

    public void checkColumn(Column column, KeysType keysType) {
        if (indexType == IndexType.BITMAP) {
            String indexColName = column.getName();
            PrimitiveType colType = column.getPrimitiveType();
            if (!(colType.isDateType() ||
                    colType.isFixedPointType() || colType.isDecimalV3Type() ||
                    colType.isStringType() || colType == PrimitiveType.BOOLEAN)) {
                throw new SemanticException(colType + " is not supported in bitmap index. "
                        + "invalid column: " + indexColName);
            } else if ((keysType == KeysType.AGG_KEYS || keysType == KeysType.UNIQUE_KEYS) && !column.isKey()) {
                throw new SemanticException(
                        "BITMAP index only used in columns of DUP_KEYS/PRIMARY_KEYS table or key columns of"
                                + " UNIQUE_KEYS/AGG_KEYS table. invalid column: " + indexColName);
            }
        } else if (indexType == IndexType.GIN) {
            InvertedIndexUtil.checkInvertedIndexValid(column, properties, keysType);
        } else if (indexType == IndexType.NGRAMBF) {
            BloomFilterIndexUtil.checkNgramBloomFilterIndexValid(column, properties, keysType);
        } else if (indexType == IndexType.VECTOR) {
            VectorIndexUtil.checkVectorIndexValid(column, properties, keysType);
        } else {
            throw new SemanticException("Unsupported index type: " + indexType);
        }
    }

    public enum IndexType {
        BITMAP,
        GIN("GIN"),
        NGRAMBF("NGRAMBF"),
        VECTOR("VECTOR");

        IndexType(String name) {
            this.displayName = name;
        }
        IndexType() {
            this.displayName = toString();
        }
        private final String displayName;

        public String getDisplayName() {
            return displayName;
        }

        public static IndexDef.IndexType getIndexType(IndexTypeContext indexTypeContext) {
            IndexDef.IndexType index;
            if (indexTypeContext == null || indexTypeContext.BITMAP() != null) {
                index = IndexDef.IndexType.BITMAP;
            } else if (indexTypeContext.GIN() != null) {
                index = IndexDef.IndexType.GIN;
            } else if (indexTypeContext.NGRAMBF() != null) {
                index = IndexType.NGRAMBF;
            } else if (indexTypeContext.VECTOR() != null) {
                index = IndexType.VECTOR;
            } else {
                throw new ParsingException("Not specify index type");
            }
            return index;
        }

        // Whether the index type is compatible with the new metadata
        public static boolean isCompatibleIndex(IndexType indexType) {
            return indexType == GIN || indexType == VECTOR;
        }
    }
}
