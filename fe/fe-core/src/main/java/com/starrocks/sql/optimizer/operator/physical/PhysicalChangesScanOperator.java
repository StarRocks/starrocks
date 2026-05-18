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

package com.starrocks.sql.optimizer.operator.physical;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.lake.bookmark.Bookmark;
import com.starrocks.lake.bookmark.BookmarkChange;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;

public class PhysicalChangesScanOperator extends PhysicalScanOperator {

    private final Bookmark base;
    private final Bookmark head;
    private final BookmarkChange delta;

    public PhysicalChangesScanOperator(Table table,
                                       Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                       long limit,
                                       ScalarOperator predicate,
                                       Bookmark base,
                                       Bookmark head,
                                       BookmarkChange delta) {
        super(OperatorType.PHYSICAL_CHANGES_SCAN, table,
                colRefToColumnMetaMap, limit, predicate, null);
        this.base = base;
        this.head = head;
        this.delta = delta;
    }

    public Bookmark getBase() {
        return base;
    }

    public Bookmark getHead() {
        return head;
    }

    public BookmarkChange getDelta() {
        return delta;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalChangesScan(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalChangesScan(optExpression, context);
    }
}
