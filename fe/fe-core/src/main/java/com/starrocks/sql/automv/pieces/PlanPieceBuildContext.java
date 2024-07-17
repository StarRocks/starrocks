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

import com.google.common.collect.ImmutableSet;
import com.starrocks.sql.automv.column.ColumnRefToIdConverter;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PlanPieceBuildContext {
    private final PieceCommonState commonState;
    private final List<PlanPiece> dependPieces;

    private PlanPieceBuildContext(ColumnRefToIdConverter idConverter, String name, Map<String, FQTable> fqTableMap,
                                  List<PlanPiece> pieces) {
        this.commonState = new PieceCommonState(Objects.requireNonNull(idConverter), ImmutableSet.of(name),
                Objects.requireNonNull(fqTableMap));
        this.dependPieces = Objects.requireNonNull(pieces);
    }

    private PlanPieceBuildContext(PieceCommonState commonState,
                                  List<PlanPiece> pieces) {
        this.commonState = Objects.requireNonNull(commonState);
        this.dependPieces = Objects.requireNonNull(pieces);
    }

    public static PlanPieceBuildContext of(ColumnRefToIdConverter idConverter,
                                           String name,
                                           Map<String, FQTable> fqTableMap,
                                           List<PlanPiece> pieces) {
        return new PlanPieceBuildContext(idConverter, name, fqTableMap, pieces);
    }

    PlanPieceBuildContext newContextWithPieces(List<PlanPiece> pieces) {
        return new PlanPieceBuildContext(this.commonState, pieces);
    }

    public Map<String, FQTable> getFqTableMap() {
        return commonState.getFqTableMap();
    }

    public ColumnRefToIdConverter getIdConverter() {
        return commonState.getIdConverter();
    }

    public PieceCommonState getCommonState() {
        return commonState;
    }

    public PlanPiece at(int i) {
        int idx = i < 0 ? i + dependPieces.size() : i;
        return this.dependPieces.get(idx);
    }

    public PlanPiece arg0() {
        return at(0);
    }

    public PlanPiece arg1() {
        return at(1);
    }

    public PlanPiece lastArg() {
        return at(-1);
    }
}
