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

package com.starrocks.sql.automv.lifecycle;

import com.starrocks.sql.automv.estimation.CardEstimateState;
import com.starrocks.sql.automv.estimation.CardRecord;
import com.starrocks.sql.automv.generator.PartitionPolicy;
import com.starrocks.sql.automv.lattice.Lattice;
import com.starrocks.sql.automv.lattice.MVRecommendation;
import com.starrocks.sql.automv.util.Box;
import com.starrocks.sql.automv.util.TieredList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class MVRecommendationSelector {

    private static final Logger LOG = LogManager.getLogger(MVRecommendationSelector.class);
    private final MVRecommendationSelectOptions options;

    public MVRecommendationSelector(MVRecommendationSelectOptions options) {
        this.options = Objects.requireNonNull(options);
    }

    boolean wellFitted(MVRecommendation mv) {
        CardRecord cardRecord = mv.getLatticeNode().getCard();
        CardEstimateState cardEstimateState = mv.getCardEstimateState();

        // card estimation is poor-quality, so kick out the MV
        if (cardEstimateState.getCardQuality().ordinal() > CardEstimateState.CardQuality.PASS.ordinal()) {
            return false;
        }

        // sampling ratio is too small that it can not infer card estimation of population,
        // so kick out the MV.
        if (cardEstimateState.getSamplingRatio() < 0.1) {
            return false;
        }

        double estimatedPopCardinality = cardRecord.getCardinality() / cardEstimateState.getSamplingRatio();

        boolean isPartitionedMV =
                PartitionPolicy.getPartitionColumnId(mv.getLatticeNode().getFinalAggPiece()).isPresent();
        if (isPartitionedMV) {
            return estimatedPopCardinality <= options.getPartitionedMVCardinalityMax();
        } else {
            return estimatedPopCardinality <= options.getUnpartitionedMVCardinalityMax();
        }
    }

    TieredList<MVRecommendation> eliminateSuperfluous(List<MVRecommendation> recommendations) {
        int sz = recommendations.size();
        if (sz == 0) {
            return TieredList.<MVRecommendation>genesis();
        }
        int effectiveMVLimit = options.getPerLatticeMVLimit();
        effectiveMVLimit = effectiveMVLimit <= 0 ? sz : effectiveMVLimit;
        int minSelectedMVs = Math.min(effectiveMVLimit, 3);
        int numMVs = (int) (Math.round(sz * options.getPerLatticeMVSelectivityRatio()));
        int numSelectedMVs = Math.min(sz, Math.max(minSelectedMVs, Math.min(numMVs, effectiveMVLimit)));

        if (sz <= numSelectedMVs) {
            return TieredList.<MVRecommendation>genesis().concat(recommendations);
        } else {
            return TieredList.<MVRecommendation>genesis().concat(recommendations.subList(0, numSelectedMVs));
        }
    }

    public TieredList<MVRecommendation> select(List<MVRecommendation> recommendations) {
        Map<Box<Lattice>, List<MVRecommendation>> latticeToMVList = recommendations
                .stream()
                .filter(this::wellFitted)
                .collect(Collectors.groupingBy(mv -> Box.of(mv.getLatticeNode().getLattice())));

        int inputNumMVs = recommendations.size();
        int outputNumWellFittedMVs = latticeToMVList.values()
                .stream()
                .map(Collection::size)
                .reduce(0, Integer::sum);
        TieredList<MVRecommendation> selectedMVs = latticeToMVList.values().stream()
                .map(this::eliminateSuperfluous)
                .reduce(TieredList.<MVRecommendation>genesis(), TieredList::concat);
        int outputSelectedMVs = selectedMVs.size();
        LOG.info("Select MVs: inputNumMVs={}, outputNumWellFittedMVs={}, outputSelectedMVs={}",
                inputNumMVs,
                outputNumWellFittedMVs,
                outputSelectedMVs);
        return selectedMVs;
    }
}
