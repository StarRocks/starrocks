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

import com.google.api.client.util.Lists;
import com.starrocks.sql.automv.estimation.CardEstimateState;
import com.starrocks.sql.automv.generator.QueryGenerateResult;
import com.starrocks.sql.automv.util.TieredList;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class MVRecommendation implements Comparable<MVRecommendation> {

    private LatticeNode latticeNode;
    private List<QueryGenerateResult> mvResultList;
    private CardEstimateState cardEstimateState;
    private double totalBenefit;
    private int numQueriesAccelerated;
    private boolean processed;
    private List<TentativeQueryBenefit> tentativeBenefits;

    public MVRecommendation(LatticeNode latticeNode) {
        this.latticeNode = latticeNode;
    }

    public MVRecommendation(QueryGenerateResult mvResult) {
        this.mvResultList = Collections.singletonList(mvResult);
    }

    public List<QueryGenerateResult> getMvResultList() {
        return mvResultList;
    }

    public void setMvResultList(List<QueryGenerateResult> mvResultList) {
        this.mvResultList = mvResultList;
    }

    public CardEstimateState getCardEstimateState() {
        return cardEstimateState;
    }

    public void setCardEstimateState(CardEstimateState cardEstimateState) {
        this.cardEstimateState = cardEstimateState;
    }

    public LatticeNode getLatticeNode() {
        return latticeNode;
    }

    public double getTotalBenefit() {
        int numDimensions = latticeNode.getId().size();
        return numDimensions <= 5 ? totalBenefit :
                totalBenefit / (Math.expm1(Math.log1p(0.2) * (numDimensions - 4)) + 1);
    }

    public void setTotalBenefit(double totalBenefit) {
        this.totalBenefit = totalBenefit;
    }

    public boolean isProcessed() {
        return processed;
    }

    public void setProcessed(boolean processed) {
        this.processed = processed;
    }

    public List<TentativeQueryBenefit> getTentativeBenefits() {
        return tentativeBenefits;
    }

    public void setTentativeBenefits(List<TentativeQueryBenefit> tentativeBenefits) {
        this.tentativeBenefits = tentativeBenefits;
    }

    public int getNumQueriesAccelerated() {
        return numQueriesAccelerated;
    }

    public void setNumQueriesAccelerated(int numQueriesAccelerated) {
        this.numQueriesAccelerated = numQueriesAccelerated;
    }

    public List<List<String>> getRowList(Supplier<Integer> idAssigner) {
        List<List<String>> rowList = Lists.newArrayListWithCapacity(mvResultList.size());
        TieredList.Builder<String> cardEstimateListBuilder = TieredList.<String>newGenesisTier();
        if (cardEstimateState != null) {
            cardEstimateListBuilder.add(cardEstimateState.getHaltReason().name());
            cardEstimateListBuilder.add("" + cardEstimateState.getTimeUsage());
            cardEstimateListBuilder.add("" + cardEstimateState.getSamplingRatio());
            cardEstimateListBuilder.add("" + cardEstimateState.getCalcSteps());
            cardEstimateListBuilder.add("" + cardEstimateState.getCardQuality().name());
        } else {
            cardEstimateListBuilder.add(CardEstimateState.HaltReason.REACH_LIMIT.name());
            cardEstimateListBuilder.add("-1");
            cardEstimateListBuilder.add("-1.0");
            cardEstimateListBuilder.add("-1");
            cardEstimateListBuilder.add(CardEstimateState.CardQuality.FAIL.name());
        }
        TieredList<String> cardEstimateList = cardEstimateListBuilder.build();

        TieredList.Builder<String> nodeInfoListBuilder = TieredList.<String>newGenesisTier();
        if (latticeNode != null) {
            nodeInfoListBuilder.add("" + latticeNode.getCard().getRowCount());
            nodeInfoListBuilder.add("" + latticeNode.getCard().getCardinality());
            nodeInfoListBuilder.add("" + latticeNode.getCard().getCardRowCountRatio());
            nodeInfoListBuilder.add("" + latticeNode.getCard().getBenefit());
            nodeInfoListBuilder.add("" + numQueriesAccelerated);
            nodeInfoListBuilder.add("" + totalBenefit);
        } else {
            nodeInfoListBuilder.add("-1");
            nodeInfoListBuilder.add("-1");
            nodeInfoListBuilder.add("-1.0");
            nodeInfoListBuilder.add("-1.0");
            nodeInfoListBuilder.add("-1");
            nodeInfoListBuilder.add("-1.0");
        }
        TieredList<String> nodeInfoList = nodeInfoListBuilder.build();

        for (QueryGenerateResult queryGenerateResult : mvResultList) {
            TieredList.Builder<String> rowBuilder = TieredList.<String>newGenesisTier();
            rowBuilder.add("" + idAssigner.get());
            rowBuilder.add(queryGenerateResult.getMvName());
            rowBuilder.add(queryGenerateResult.getSubquery().getResult());
            List<String> row = rowBuilder.build().concat(cardEstimateList).concat(nodeInfoList);
            rowList.add(row);
        }
        return rowList;
    }

    @Override
    public int compareTo(@NotNull MVRecommendation that) {
        int r = Double.compare(that.totalBenefit, this.totalBenefit);
        if (r != 0) {
            return r;
        } else {
            if (this.getCardEstimateState() != null && that.getCardEstimateState() != null) {
                return that.getCardEstimateState().getCardQuality()
                        .compareTo(this.getCardEstimateState().getCardQuality());
            } else {
                return 0;
            }
        }
    }
}
