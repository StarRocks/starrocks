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

package com.starrocks.sql.automv.lattice;

import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.automv.column.ColumnRefToIdConverter;
import com.starrocks.sql.automv.generator.MVGenerateContext;
import com.starrocks.sql.automv.generator.MVName;
import com.starrocks.sql.automv.generator.OneOneMVGenerator;
import com.starrocks.sql.automv.options.AutoMVOptions;
import com.starrocks.sql.automv.pieces.PlanPiece;
import com.starrocks.sql.automv.pieces.TableUsage;
import com.starrocks.sql.automv.qe.RboOptimizer;
import com.starrocks.sql.automv.tunespace.PlanPieceInfo;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class OneOneMVRecommender implements MVRecommender {
    private final ConnectContext ctx;
    private final AutoMVOptions options;

    public OneOneMVRecommender(ConnectContext ctx, AutoMVOptions options) {
        this.ctx = Objects.requireNonNull(ctx);
        this.options = Objects.requireNonNull(options);
    }

    private Optional<MVRecommendation> recommendOne(TableUsage tableUsage) {
        ColumnRefToIdConverter idConverter = tableUsage.getPiece().getCommonState().getIdConverter();
        MVGenerateContext mvGenerateContext = MVGenerateContext.builder()
                .setMvNameGenerator(query -> MVName.generateFromQuery(query).toString())
                .setNextId(idConverter::nextId)
                .setOptions(options)
                .build();

        return OneOneMVGenerator.generate(tableUsage, mvGenerateContext)
                .map(MVRecommendation::new);
    }

    @Override
    public List<PlanPiece> getPlanPieces(List<PlanPieceInfo> pieceInfos) {
        return pieceInfos.stream()
                .map(pieceInfo -> Pair.create(pieceInfo.getName(), pieceInfo.getQuery()))
                .flatMap(p -> RboOptimizer.get11MVPlanPieces(p.first, p.second, ctx).stream())
                .collect(Collectors.toList());
    }

    public List<MVRecommendation> recommend(List<PlanPiece> pieces, int startIdx, int endIdx) {
        List<TableUsage> tableUsages = pieces.stream()
                .map(TableUsage::analyzeUsage)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        List<TableUsage> mergedTableUsages = TableUsage.mergeUsages(tableUsages);
        return mergedTableUsages.stream().map(this::recommendOne)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }
}
