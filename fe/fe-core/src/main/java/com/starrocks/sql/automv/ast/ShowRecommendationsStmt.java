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

package com.starrocks.sql.automv.ast;

import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.epack.sql.ast.AstVisitorEPack;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.parser.NodePosition;

import java.util.Optional;

public class ShowRecommendationsStmt extends ShowStmt {
    private static final String ID = "Id";
    private static final String MV_NAME = "Name";
    private static final String RECOMMENDED_MV = "RecommendedMV";
    private static final String HALT_REASON = "HaltReason";
    private static final String TIME_USAGE = "TimeUsage";
    private static final String SAMPLING_RATIO = "SamplingRatio";
    private static final String CALC_STEPS = "CalcSteps";
    private static final String CARD_QUALITY = "CardQuality";
    private static final String ROW_COUNT = "RowCount";
    private static final String CARDINALITY = "Cardinality";
    private static final String CARD_ROW_COUNT_RATIO = "CardRowCountRatio";
    private static final String BENEFIT = "Benefit";
    private static final String NUM_QUERIES_ACCELERATED = "NumQueriesAccelerated";
    private static final String TOTAL_BENEFIT = "TotalBenefit";
    private static final String ACCELERATED_QUERIES = "AcceleratedQueries";
    private static final ShowResultSetMetaData META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column(ID, Type.INT))
            .addColumn(new Column(MV_NAME, Type.STRING))
            .addColumn(new Column(RECOMMENDED_MV, Type.STRING))
            .addColumn(new Column(HALT_REASON, Type.STRING))
            .addColumn(new Column(TIME_USAGE, Type.BIGINT))
            .addColumn(new Column(SAMPLING_RATIO, Type.DOUBLE))
            .addColumn(new Column(CALC_STEPS, Type.INT))
            .addColumn(new Column(CARD_QUALITY, Type.STRING))
            .addColumn(new Column(ROW_COUNT, Type.BIGINT))
            .addColumn(new Column(CARDINALITY, Type.BIGINT))
            .addColumn(new Column(CARD_ROW_COUNT_RATIO, Type.DOUBLE))
            .addColumn(new Column(BENEFIT, Type.DOUBLE))
            .addColumn(new Column(NUM_QUERIES_ACCELERATED, Type.INT))
            .addColumn(new Column(TOTAL_BENEFIT, Type.DOUBLE))
            .addColumn(new Column(ACCELERATED_QUERIES, Type.STRING))
            .build();
    private TableName tableName;
    private boolean single = false;
    private long limit = -1;
    private long offset = -1;

    public ShowRecommendationsStmt(TableName tableName, long limit, long offset) {
        super(NodePosition.ZERO);
        this.tableName = tableName;
        this.limit = limit;
        this.offset = offset;
    }

    public boolean isSingle() {
        return single;
    }

    public void setSingle(boolean single) {
        this.single = single;
    }

    public TableName getTableName() {
        return tableName;
    }

    public void setTableName(TableName tableName) {
        this.tableName = tableName;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    public Optional<Long> getLimit() {
        return Optional.ofNullable(limit < 0 ? null : limit);
    }

    public void setLimit(Long limit) {
        this.limit = limit;
    }

    public Optional<Long> getOffset() {
        return Optional.ofNullable(offset < 0 ? null : offset);
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        if (visitor instanceof AstVisitorEPack) {
            return ((AstVisitorEPack<R, C>) visitor).visitShowRecommendationsStmt(this, context);
        } else {
            return null;
        }
    }
}
