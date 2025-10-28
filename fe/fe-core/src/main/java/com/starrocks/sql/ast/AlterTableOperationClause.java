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

import com.starrocks.alter.AlterOpType;
import com.starrocks.analysis.Expr;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public class AlterTableOperationClause extends AlterTableClause {
    public class RewriteDataOptions {
        private Expr where;
        private boolean rewriteAll;
        private long minFileSizeBytes;
        private long batchSize;
        private long batchParallelism;
        private ScalarOperator partitionFilter;
    
        public RewriteDataOptions(Expr where,
                                boolean rewriteAll,
                                long minFileSizeBytes,
                                long batchSize,
                                long batchParallelism) {
            this.where           = where;
            this.rewriteAll      = rewriteAll;
            this.minFileSizeBytes = minFileSizeBytes;
            this.batchSize       = batchSize;
            this.batchParallelism = batchParallelism;
            this.partitionFilter   = null;
        }
    }
    private final String tableOperationName;
    private final List<Expr> exprs;
    private List<ConstantOperator> args;
    private RewriteDataOptions rewriteDataOptions;

    public AlterTableOperationClause(NodePosition pos, String tableOperationName, List<Expr> exprs, Expr where) {
        super(AlterOpType.ALTER_TABLE_OPERATION, pos);
        this.tableOperationName = tableOperationName;
        this.exprs = exprs;
        rewriteDataOptions = new RewriteDataOptions(where, false, 256L * 1024 * 1024, 10L * 1024 * 1024 * 1024, 1);
    }


    public String getTableOperationName() {
        return tableOperationName;
    }

    public List<Expr> getExprs() {
        return exprs;
    }

    public List<ConstantOperator> getArgs() {
        return args;
    }

    public void setArgs(List<ConstantOperator> args) {
        this.args = args;
    }

    public void setWhere(Expr where) {
        this.rewriteDataOptions.where = where;
    }

    public Expr getWhere() {
        return rewriteDataOptions.where;
    }

    public void setRewriteAll(boolean rewriteAll) {
        this.rewriteDataOptions.rewriteAll = rewriteAll;
    }

    public boolean isRewriteAll() {
        return this.rewriteDataOptions.rewriteAll;
    }

    public void setMinFileSizeBytes(long minFileSizeBytes) {
        this.rewriteDataOptions.minFileSizeBytes = minFileSizeBytes;
    }

    public long getMinFileSizeBytes() {
        return this.rewriteDataOptions.minFileSizeBytes;
    }

    public void setBatchSize(long batchSize) {
        this.rewriteDataOptions.batchSize = batchSize;
    }

    public long getBatchSize() {
        return this.rewriteDataOptions.batchSize;
    }

    public void setBatchParallelism(long batchParallelism) {
        this.rewriteDataOptions.batchParallelism = batchParallelism;
    }

    public long getBatchParallelism() {
        return this.rewriteDataOptions.batchParallelism;
    }

    public void setPartitionFilter(ScalarOperator partitionFilter) {
        this.rewriteDataOptions.partitionFilter = partitionFilter;
    }
    
    public ScalarOperator getPartitionFilter() {
        return this.rewriteDataOptions.partitionFilter;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterTableOperationClause(this, context);
    }
}
