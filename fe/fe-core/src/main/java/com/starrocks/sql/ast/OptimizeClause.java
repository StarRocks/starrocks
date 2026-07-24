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

import com.google.common.collect.Lists;
<<<<<<< HEAD
import com.google.gson.annotations.SerializedName;
import com.starrocks.alter.AlterOpType;
=======
import com.starrocks.common.util.SqlUtils;
import com.starrocks.sql.ast.expression.StringLiteral;
>>>>>>> 7e9b5d45e4 ([BugFix] Fix Operation column showing memory address in SHOW ALTER TABLE OPTIMIZE (#75948))
import com.starrocks.sql.parser.NodePosition;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;

public class OptimizeClause extends AlterTableClause {
    private KeysDesc keysDesc;
    private PartitionDesc partitionDesc;
    private DistributionDesc distributionDesc;
    private PartitionNames partitionNames;
    private OptimizeRange range;

    @SerializedName(value = "sourcePartitionIds")
    private List<Long> sourcePartitionIds = Lists.newArrayList();

    @SerializedName(value = "isTableOptimize")
    private boolean isTableOptimize = false;

    private List<String> sortKeys = null;

    public OptimizeClause(KeysDesc keysDesc,
                          PartitionDesc partitionDesc,
                          DistributionDesc distributionDesc,
                          List<String> sortKeys,
                          PartitionNames partitionNames,
                          OptimizeRange range) {
        this(keysDesc, partitionDesc, distributionDesc, sortKeys, partitionNames, range, NodePosition.ZERO);
    }

    public OptimizeClause(KeysDesc keysDesc,
                          PartitionDesc partitionDesc,
                          DistributionDesc distributionDesc,
                          List<String> sortKeys,
                          PartitionNames partitionNames,
                          OptimizeRange range,
                          NodePosition pos) {
        super(AlterOpType.OPTIMIZE, pos);
        this.keysDesc = keysDesc;
        this.partitionDesc = partitionDesc;
        this.distributionDesc = distributionDesc;
        this.sortKeys = sortKeys;
        this.partitionNames = partitionNames;
        this.range = range;
    }

    // Add getter and setter for OptimizeRange
    public OptimizeRange getRange() {
        return range;
    }

    public void setRange(OptimizeRange range) {
        this.range = range;
    }

    public KeysDesc getKeysDesc() {
        return this.keysDesc;
    }

    public PartitionDesc getPartitionDesc() {
        return this.partitionDesc;
    }

    public DistributionDesc getDistributionDesc() {
        return this.distributionDesc;
    }

    public List<String> getSortKeys() {
        return sortKeys;
    }

    public void setDistributionDesc(DistributionDesc distributionDesc) {
        this.distributionDesc = distributionDesc;
    }

    public void setPartitionDesc(PartitionDesc partitionDesc) {
        this.partitionDesc = partitionDesc;
    }

    public PartitionNames getPartitionNames() {
        return partitionNames;
    }

    public void setSourcePartitionIds(List<Long> sourcePartitionIds) {
        this.sourcePartitionIds = sourcePartitionIds;
    }

    public List<Long> getSourcePartitionIds() {
        return sourcePartitionIds;
    }

    public boolean isTableOptimize() {
        return isTableOptimize;
    }

    public void setTableOptimize(boolean tableOptimize) {
        isTableOptimize = tableOptimize;
    }

    public static OptimizeClause read(DataInput in) throws IOException {
        throw new RuntimeException("OptimizeClause serialization is not supported anymore.");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER ");
        if (partitionDesc != null) {
            sb.append(partitionDesc.toString());
        }
        if (distributionDesc != null) {
            sb.append(distributionDesc.toString());
        }
        if (keysDesc != null) {
            sb.append(keysDesc.toSql());
        }
        if (sortKeys != null && !sortKeys.isEmpty()) {
            sb.append(String.join(",", sortKeys));
        }
        if (range != null) {
            sb.append(range.toString());
        }
        return sb.toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitOptimizeClause(this, context);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();

        // PARTITIONS (p1, p2)
        if (partitionNames != null && !partitionNames.getPartitionNames().isEmpty()) {
            if (!sb.isEmpty()) {
                sb.append(" ");
            }
            sb.append(partitionNames.toString());
        }

        // DUPLICATE KEY(`col1`, `col2`) etc.
        if (keysDesc != null) {
            if (!sb.isEmpty()) {
                sb.append(" ");
            }
            sb.append(keysDesc.getKeysType().toSql());
            List<String> keyColumns = keysDesc.getKeysColumnNames();
            if (keyColumns != null && !keyColumns.isEmpty()) {
                sb.append("(");
                for (int i = 0; i < keyColumns.size(); i++) {
                    if (i != 0) {
                        sb.append(", ");
                    }
                    sb.append(SqlUtils.getIdentSql(keyColumns.get(i)));
                }
                sb.append(")");
            }
        }

        // PARTITION BY RANGE(...)
        if (partitionDesc != null) {
            if (!sb.isEmpty()) {
                sb.append(" ");
            }
            sb.append(partitionDesc);
        }

        // ORDER BY (`col1`, `col2`)
        if (sortKeys != null && !sortKeys.isEmpty()) {
            if (!sb.isEmpty()) {
                sb.append(" ");
            }
            sb.append("ORDER BY (");
            for (int i = 0; i < sortKeys.size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                sb.append(SqlUtils.getIdentSql(sortKeys.get(i)));
            }
            sb.append(")");
        }

        // DISTRIBUTED BY HASH(`col1`) BUCKETS 10
        if (distributionDesc != null) {
            if (!sb.isEmpty()) {
                sb.append(" ");
            }
            if (distributionDesc instanceof HashDistributionDesc) {
                HashDistributionDesc hashDesc = (HashDistributionDesc) distributionDesc;
                sb.append("DISTRIBUTED BY HASH(");
                List<String> distCols = hashDesc.getDistributionColumnNames();
                for (int i = 0; i < distCols.size(); i++) {
                    if (i != 0) {
                        sb.append(", ");
                    }
                    sb.append(SqlUtils.getIdentSql(distCols.get(i)));
                }
                sb.append(")");
                if (hashDesc.getBuckets() > 0) {
                    sb.append(" BUCKETS ").append(hashDesc.getBuckets());
                }
            } else if (distributionDesc instanceof RandomDistributionDesc) {
                RandomDistributionDesc randomDesc = (RandomDistributionDesc) distributionDesc;
                sb.append("DISTRIBUTED BY RANDOM");
                if (randomDesc.getBuckets() > 0) {
                    sb.append(" BUCKETS ").append(randomDesc.getBuckets());
                }
            } else {
                sb.append(distributionDesc);
            }
        }

        // BETWEEN start AND end
        if (range != null) {
            if (!sb.isEmpty()) {
                sb.append(" ");
            }
            sb.append("BETWEEN ");
            if (range.getStart() != null) {
                sb.append(toSqlStringLiteral(range.getStart())).append(" ");
            }
            sb.append("AND");
            if (range.getEnd() != null) {
                sb.append(" ").append(toSqlStringLiteral(range.getEnd()));
            }
        }

        return sb.toString();
    }

    /**
     * Render a {@link StringLiteral} as a single-quoted,
     * backslash-and-quote-escaped SQL string. Using {@code getStringValue()} directly would drop the
     * surrounding quotes (and the escaping), producing e.g. {@code BETWEEN 2024-01-01 AND 2024-12-31}
     * which is neither the submitted clause nor valid for the {@code optimizeRange} grammar.
     */
    private static String toSqlStringLiteral(StringLiteral literal) {
        return "'" + SqlUtils.escapeSqlString(literal.getStringValue()) + "'";
    }

    @Override
    public String toString() {
        return toSql();
    }
}
