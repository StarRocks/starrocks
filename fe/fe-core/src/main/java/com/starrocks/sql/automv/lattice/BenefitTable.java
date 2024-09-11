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

import com.google.common.collect.ImmutableList;
import com.starrocks.common.Pair;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.util.TieredList;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;

public final class BenefitTable {
    private final List<MVRecommendation> candidateMVs;
    private final List<QueryBenefit> queryBenefits;

    public BenefitTable(List<MVRecommendation> candiMVBenefits, List<QueryBenefit> queryBenefits) {
        this.candidateMVs = Objects.requireNonNull(candiMVBenefits);
        this.queryBenefits = Objects.requireNonNull(queryBenefits);
        candidateMVs.forEach(candiMV -> computeTentativeBenefit(candiMV, queryBenefits));
    }

    public static void computeTentativeBenefit(MVRecommendation mvRecommendation, List<QueryBenefit> queryBenefits) {
        List<TentativeQueryBenefit> tentativeBenefits = IntStream.range(0, queryBenefits.size())
                .mapToObj(i -> Pair.create(i, queryBenefits.get(i)))
                .filter(p -> canRewrite(mvRecommendation.getLatticeNode(), p.second))
                .map(p -> new TentativeQueryBenefit(p.second.getId(), p.first))
                .collect(ImmutableList.toImmutableList());
        mvRecommendation.setTentativeBenefits(tentativeBenefits);
    }

    private static boolean canRewrite(LatticeNode node, QueryBenefit queryBenefit) {
        AggregatePiece piece = node.getFinalAggPiece();
        LatticeNode.Category pieceCategory = LatticeNode.Category.getCategory(piece);
        boolean isUncoverII = pieceCategory.equals(LatticeNode.Category.UNCOVERABLE_II);
        switch (queryBenefit.getCategory()) {
            case COVERABLE:
                return !isUncoverII && node.getId().isCovering(queryBenefit.getId());
            case UNCOVERABLE_I:
                return !isUncoverII && node.getId().equals(queryBenefit.getId());
            case UNCOVERABLE_II: {
                String norm = piece.getFlatTable().getFlexibleConjunctsNormHash();
                boolean identicalNorm = Objects.equals(norm, queryBenefit.getConjunctsNormHash());
                return isUncoverII && node.getId().equals(queryBenefit.getId()) && identicalNorm;
            }
        }
        return false;
    }

    public List<MVRecommendation> getCandidateMVs() {
        return candidateMVs;
    }

    public List<QueryBenefit> getQueryBenefits() {
        return queryBenefits;
    }

    private boolean calculateOnce() {
        for (MVRecommendation candiMV : candidateMVs) {
            if (candiMV.isProcessed()) {
                continue;
            }
            double mvCost = candiMV.getLatticeNode().getCard().getCardinality();
            double totalBenefit = 0.0;
            for (TentativeQueryBenefit tqBenefit : candiMV.getTentativeBenefits()) {
                int idx = tqBenefit.getIndex();
                QueryBenefit qBenefit = queryBenefits.get(idx);
                double prevMvCost = qBenefit.getCost();
                boolean selected = prevMvCost > mvCost;
                tqBenefit.setSelected(selected);
                if (selected) {
                    tqBenefit.setCost(mvCost);
                    double benefit = prevMvCost - mvCost;
                    tqBenefit.setBenefit(benefit);
                    totalBenefit += benefit * qBenefit.getWeight();
                }
            }
            candiMV.setTotalBenefit(totalBenefit);
        }

        Optional<MVRecommendation> optMostPromisingCandidateMV = candidateMVs.stream()
                .filter(candiMV -> !candiMV.isProcessed())
                .max(Comparator.comparingDouble(MVRecommendation::getEffectiveTotalBenefit));

        optMostPromisingCandidateMV.ifPresent(candiMV -> {
            candiMV.setProcessed(true);
            int numQueriesAccelerated = 0;
            for (TentativeQueryBenefit tqBenefit : candiMV.getTentativeBenefits()) {
                int idx = tqBenefit.getIndex();
                QueryBenefit qBenefit = queryBenefits.get(idx);
                qBenefit.setBenefit(qBenefit.getBenefit());
                qBenefit.setCost(qBenefit.getCost());
                qBenefit.setUsedLatticeId(candiMV.getLatticeNode().getId());
                numQueriesAccelerated += Double.valueOf(qBenefit.getWeight()).intValue();
            }
            candiMV.setNumQueriesAccelerated(numQueriesAccelerated);
        });

        return optMostPromisingCandidateMV.isPresent();
    }

    public TieredList<MVRecommendation> calculate() {
        while (calculateOnce()) {
        }
        return candidateMVs.stream().filter(MVRecommendation::isProcessed).collect(TieredList.toList());
    }
}