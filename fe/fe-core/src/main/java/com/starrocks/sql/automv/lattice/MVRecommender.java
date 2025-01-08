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
import com.starrocks.sql.automv.options.AutoMVOptions;
import com.starrocks.sql.automv.pieces.PlanPiece;
import com.starrocks.sql.automv.tunespace.PlanPieceInfo;

import java.util.List;

public interface MVRecommender {

    static MVRecommender createMVRecommender(Type type, ConnectContext ctx, AutoMVOptions options) {
        switch (type) {
            case SPJG_MV:
                return new SPJGMVRecommender(ctx, options);
            case ONE_ONE_MV:
                return new OneOneMVRecommender(ctx, options);
            default:
        }
        return null;
    }

    default List<MVRecommendation> recommendFromPieceInfos(List<PlanPieceInfo> pieceInfos, int startIdx, int endIdx) {
        return recommend(getPlanPieces(pieceInfos), startIdx, endIdx);
    }

    List<MVRecommendation> recommend(List<PlanPiece> pieces, int startIdx, int endIdx);

    List<PlanPiece> getPlanPieces(List<PlanPieceInfo> pieceInfos);

    enum Type {
        ONE_ONE_MV,
        SPJG_MV,
    }
}