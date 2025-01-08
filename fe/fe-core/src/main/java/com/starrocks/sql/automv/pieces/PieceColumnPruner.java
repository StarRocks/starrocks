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

import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.optimizer.base.ColumnRefSet;

import java.util.List;
import java.util.stream.Collectors;

// PieceColumnPruner is used to prune columns of PlanPiece in bottom-up
// style, since both 11-MV and SPJG use PlanPieceBuilder to convert
// LogicalPlan into PlanPiece, PlanPieceBuilder would add all columns of
// the tables into PlanPiece to create a logical wide table, in this way,
// MV merging is simplified; however, in 11-MV, we only output used columns
// of tables.
public class PieceColumnPruner {
    private static final Visitor VISITOR = new Visitor();

    public static PlanPiece prune(PlanPiece piece) {
        return VISITOR.processBottomUp(piece);
    }

    private static final class Visitor extends PlanPieceVisitor<PlanPiece, Void> {

        @Override
        public PlanPiece visitTable(TablePiece tablePiece, Void context) {
            TieredMap<Integer, GenericColumn> newColumns = tablePiece.getColumns().entrySet()
                    .stream()
                    .filter(e -> e.getValue().isDerived() || tablePiece.getUsedColumns().contains(e.getKey()))
                    .collect(TieredMap.toMap());
            return tablePiece.builder().setColumns(newColumns).build();
        }

        @Override
        public PlanPiece visitPlanPiece(PlanPiece piece, Void context) {
            ColumnRefSet usedColumns = ColumnRefSet.of();

            piece.getInputPieces()
                    .stream()
                    .map(inputPiece -> ColumnRefSet.createByIds(inputPiece.getColumns().keySet()))
                    .forEach(usedColumns::union);

            TieredMap<Integer, GenericColumn> newColumns = piece.getColumns().entrySet()
                    .stream()
                    .filter(e -> e.getValue().isDerived() || usedColumns.contains(e.getKey()))
                    .collect(TieredMap.toMap());

            return piece.builder().setColumns(newColumns).build();
        }

        @Override
        public PlanPiece visitAggregate(AggregatePiece aggPiece, Void context) {
            return aggPiece;
        }

        private PlanPiece processBottomUp(PlanPiece piece) {
            List<PlanPiece> newInputPieces = piece.getInputPieces()
                    .stream()
                    .map(this::processBottomUp)
                    .collect(Collectors.toList());
            return piece.replaceInputPieces(newInputPieces).accept(this, null);
        }
    }
}
