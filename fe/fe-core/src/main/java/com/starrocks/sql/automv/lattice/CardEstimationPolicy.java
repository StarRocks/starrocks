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

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.automv.estimation.CardEstimateState;
import com.starrocks.sql.automv.estimation.CardEstimator;
import com.starrocks.sql.automv.estimation.CardRecord;
import com.starrocks.sql.automv.options.AutoMVOptions;
import com.starrocks.sql.automv.util.TieredList;

import java.util.Map;
import java.util.Objects;

public class CardEstimationPolicy {
    private final AutoMVOptions options;
    private final ConnectContext context;

    public CardEstimationPolicy(AutoMVOptions options,
                                ConnectContext context) {
        this.options = Objects.requireNonNull(options);
        this.context = Objects.requireNonNull(context);
    }

    public TieredList<MVRecommendation> estimate(Lattice lattice) {
        // consolidation merging
        lattice.consolidate();
        // covering merging
        // TODO(by satanson): covering merge can generate lots of LatticeNode, we should
        //  limit the number of extending LatticeNodes.
        lattice.addAllMinimalCoveringNodes();
        // add a cover-all AggregatePiece
        lattice.addMaximalNode();

        CardEstimator cardEstimator = new CardEstimator(options, lattice);
        CardEstimateState state = cardEstimator.converge(context);
        Map<LatticeNodeId, CardRecord> cards = cardEstimator.getEstimatedCards();
        lattice.getNodes().forEach(node -> node.setCard(cards.get(node.getId())));
        TieredList<MVRecommendation> recommendations = lattice.pickupRecommendations(options);
        recommendations.forEach(rec -> rec.setCardEstimateState(state));
        return recommendations;
    }
}
