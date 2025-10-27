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
import com.starrocks.alter.AlterOpType;
import com.starrocks.analysis.TableName;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

/**
 * ALTER TABLE tbl DISTRIBUTE BY HASH(col1, col2, ...) [DEFAULT] BUCKETS N;
 * Only modifies the table's defaultDistributionInfo bucket number (must be > 0) and
 * requires the hash column list to match existing distribution columns exactly.
 */
public class AlterTableModifyDefaultBucketsClause extends AlterTableClause {
    private final List<String> distributionColumns;
    private final int bucketNum;
    protected TableName tableName;

    public AlterTableModifyDefaultBucketsClause(List<String> distributionColumns, int bucketNum, NodePosition pos) {
        super(AlterOpType.MODIFY_TABLE_PROPERTY_SYNC, pos);
        Preconditions.checkArgument(bucketNum > 0, "bucket num must > 0");
        this.distributionColumns = distributionColumns;
        this.bucketNum = bucketNum;
    }

    public List<String> getDistributionColumns() {
        return distributionColumns;
    }

    public int getBucketNum() {
        return bucketNum;
    }

    public void setTableName(TableName tableName) {
        this.tableName = tableName;
    }

    public TableName getTableName() {
        return tableName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterTableModifyDefaultBucketsClause(this, context);
    }
}
