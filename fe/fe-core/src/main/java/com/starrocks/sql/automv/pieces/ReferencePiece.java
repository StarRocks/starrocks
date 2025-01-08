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

import com.google.common.collect.ImmutableList;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.pn.Op;
import com.starrocks.sql.automv.pn.OpUtil;
import com.starrocks.sql.automv.util.TieredList;
import com.starrocks.sql.automv.util.TieredMap;

import java.util.List;
import java.util.Map;

public class ReferencePiece extends PlanPiece {
    private ReferencePiece(List<PlanPiece> inputPieces,
                           TieredMap<Integer, GenericColumn> columns,
                           TieredList<Op> conjuncts,
                           PieceCommonState commonState, PieceAuxState auxState) {
        super(inputPieces, columns, conjuncts, commonState, auxState);
    }

    public static ReferencePiece of(PlanPiece inputPiece) {
        TieredMap<Integer, GenericColumn> newColumns = inputPiece.getColumns().entrySet()
                .stream()
                .collect(TieredMap.toMap(Map.Entry::getKey, e -> OpUtil.ref(e.getKey(), e.getValue())));
        return new ReferencePiece(ImmutableList.of(inputPiece), newColumns, TieredList.<Op>genesis(),
                inputPiece.getCommonState(), inputPiece.getAuxState());
    }

    @Override
    public Builder<? extends PlanPiece> builder() {
        throw new UnsupportedOperationException();
    }

    @Override
    public PlanPiece replaceInputPieces(List<PlanPiece> pieces) {
        return pieces.get(0);
    }

    @Override
    public PlanPiece setConjuncts(TieredList<Op> conjuncts) {
        PlanPiece newInputPiece = getInputPieces().get(0).setConjuncts(conjuncts);
        return ReferencePiece.of(newInputPiece);
    }

    @Override
    public TieredList<Op> getConjuncts() {
        return getInputPieces().get(0).getConjuncts();
    }
}
