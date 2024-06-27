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
import com.starrocks.analysis.LimitElement;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;

import java.util.Optional;

public class RecommendDataCacheSelectStmt extends ShowStmt {
    // if target is empty, means recommend cache select from whole database
    private final Optional<QualifiedName> target;
    // only interval >= 0 is valid
    // interval > 0 means that cache select is recommended based on the data in the most recent ${interval} seconds.
    private final long interval;
    private final LimitElement limitElement;

    public RecommendDataCacheSelectStmt(QualifiedName target, long interval, LimitElement limitElement, NodePosition pos) {
        super(pos);
        Preconditions.checkArgument(interval >= 0, "Interval must be >= 0");
        Preconditions.checkNotNull(limitElement, "LimitElement must not be null");
        this.target = Optional.ofNullable(target);
        this.interval = interval;
        this.limitElement = limitElement;
    }

    public Optional<QualifiedName> getTarget() {
        return target;
    }

    public long getInterval() {
        return interval;
    }

    public LimitElement getLimitElement() {
        return limitElement;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRecommendDataCacheSelectStmt(this, context);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return null;
    }
}