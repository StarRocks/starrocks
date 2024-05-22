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

package com.starrocks.sql.automv.pieces;

public abstract class PlanPieceVisitor<R, C> {
    public R visit(PlanPiece piece, C context) {
        return piece.accept(this, context);
    }

    public R visitPlanPiece(PlanPiece piece, C context) {
        return visit(piece, context);
    }

    public R visitTable(TablePiece tablePiece, C context) {
        return visitPlanPiece(tablePiece, context);
    }

    public R visitStarJoin(StarJoinPiece joinPiece, C context) {
        return visitPlanPiece(joinPiece, context);
    }

    public R visitAggregate(AggregatePiece aggPiece, C context) {
        return visitPlanPiece(aggPiece, context);
    }
}
