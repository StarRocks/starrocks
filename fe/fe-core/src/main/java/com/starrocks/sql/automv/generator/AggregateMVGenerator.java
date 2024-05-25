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

package com.starrocks.sql.automv.generator;

import com.google.api.client.util.Lists;
import com.starrocks.common.Pair;
import com.starrocks.sql.automv.column.ColumnAlias;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.sql.automv.util.TieredMap;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

// AggregateMVGenerator is used generate a textual MV schema from AggregatePiece.
// AggregateMVGenerator invokes QueryGenerator to generate MV's query,and invokes
// DistributionPolicy,PartitionPolicy, PropertiesPolicy and etc to cook clauses that
// required by MV schema.
public class AggregateMVGenerator {
    public static Optional<QueryGenerateResult> generate(AggregatePiece aggPiece, MVGenerateContext context) {
        PrettyPrinter mvSchema = new PrettyPrinter();
        QueryGenerateContext queryGenerateContext = QueryGenerateContext.of(context.isEnableGenerateTraceLog(), false);
        QueryGenerateResult result = QueryGenerator.generate(aggPiece, queryGenerateContext);
        TieredMap<Integer, ColumnAlias> columnAliases = result.getColumnAliases();

        List<String> mvColumns = result.getOrderedColumns().stream()
                .map(p -> columnAliases.get(p.first))
                .map(ColumnAlias::getName).collect(Collectors.toList());

        List<String> mvDimensionColumns = result.getOrderedDimensions().stream()
                .map(p -> columnAliases.get(p.first))
                .map(ColumnAlias::getName).collect(Collectors.toList());

        String mvName = context.getMvNameGenerator().apply(result.getSubquery().getResult());
        mvSchema.add("CREATE MATERIALIZED VIEW").spaces(1).add(mvName).spaces(1).add("(").newLine();
        mvSchema.indentEnclose(() -> mvSchema.addItemsWithNlDel(", ", mvColumns));
        mvSchema.newLine().add(")").newLine();
        mvSchema.add("COMMENT").spaces(1).addDoubleQuoted("MV recommended by AutoMV").newLine();
        Optional<PrettyPrinter> optPartitionExpr = PartitionPolicy.getPartitionExpr(aggPiece, columnAliases);
        optPartitionExpr.ifPresent(mvSchema::addSuperStep);
        mvSchema.addSuperStep(DistributionPolicy.getDistribution(aggPiece, mvDimensionColumns));
        List<Pair<Integer, GenericColumn>> candidateOrderByColumns = Lists.newArrayList();

        final int maxOrderByColumns = context.getOptions().getMaxOrderByColumns();
        for (int i = 0; i < result.getOrderedDimensions().size(); ++i) {
            Pair<Integer, GenericColumn> columnPair = result.getOrderedDimensions().get(i);
            GenericColumn column = columnPair.second;
            if (!column.getType().canDistributedBy() || candidateOrderByColumns.size() >= maxOrderByColumns) {
                break;
            }
            candidateOrderByColumns.add(columnPair);
        }
        if (candidateOrderByColumns.isEmpty()) {
            return Optional.empty();
        }

        List<String> orderByItems = candidateOrderByColumns.stream()
                .map(p -> columnAliases.get(p.first))
                .map(ColumnAlias::getName).collect(Collectors.toList());
        mvSchema.add("ORDER BY (").addItems(", ", orderByItems).add(")").newLine();
        //TODO(by satanson): At AutoMV-L2 stage, it is hard to infer a robust mv refresh policy, so
        // MV expert should specify one. in future (since AutoMV-L3 stage), a sophisticated refresh
        // policy will be developed.
        mvSchema.add("REFRESH ASYNC START(\"2023-12-01 10:00:00\") EVERY(INTERVAL 1 DAY)").newLine();
        mvSchema.addSuperStep(PropertiesPolicy.getProperties(aggPiece, columnAliases));
        mvSchema.add("AS").newLine();
        mvSchema.addSuperStep(result.getSubquery());
        QueryGenerateResult mvResult = result.updateSubquery(mvSchema)
                .setMvName(mvName)
                .setTraceLog(result.getTraceLog().orElse(null));
        return Optional.of(mvResult);
    }
}
