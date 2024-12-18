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
import com.starrocks.sql.automv.pn.Op;
import com.starrocks.sql.automv.util.TieredList;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.optimizer.base.ColumnRefSet;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class TablePiece extends PlanPiece {
    private final FQTable table;
    // Only used in 11MV
    private final ColumnRefSet usedColumns;

    private TablePiece(TieredMap<Integer, GenericColumn> columns, ColumnRefSet usedColumns, TieredList<Op> conjuncts,
                       PieceCommonState commonState, PieceAuxState auxState, FQTable table) {
        super(Collections.emptyList(), columns, conjuncts, commonState, auxState);
        this.table = Objects.requireNonNull(table);
        this.usedColumns = Objects.requireNonNull(usedColumns);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public ColumnRefSet getUsedColumns() {
        return usedColumns;
    }

    public String getTableName() {
        return table.getFQName();
    }

    public FQTable getTable() {
        return table;
    }

    @Override
    public PlanPiece.Builder<? extends PlanPiece> builder() {
        return new Builder(this);
    }

    @Override
    public <R, C> R accept(PlanPieceVisitor<R, C> visitor, C context) {
        return visitor.visitTable(this, context);
    }

    @Override
    public PlanPiece replaceInputPieces(List<PlanPiece> pieces) {
        return this.builder().build();
    }

    public static class Builder extends PlanPiece.Builder<TablePiece> {
        private FQTable table;
        private ColumnRefSet usedColumns;

        private Builder(TablePiece piece) {
            super(Collections.emptyList(), piece.getColumns(), piece.getConjuncts(), piece.getCommonState(),
                    piece.getAuxState());
            this.table = piece.table;
            this.usedColumns = piece.usedColumns;
        }

        private Builder() {
            super();
        }

        public ColumnRefSet getUsedColumns() {
            return usedColumns;
        }

        public Builder setUsedColumns(ColumnRefSet usedColumns) {
            this.usedColumns = usedColumns;
            return this;
        }

        public FQTable getTable() {
            return table;
        }

        public Builder setTable(FQTable table) {
            this.table = Objects.requireNonNull(table);
            return this;
        }

        @Override
        public TablePiece build() {
            return new TablePiece(getColumns(), getUsedColumns(), getConjuncts(), getCommonState(), getAuxState(),
                    table);
        }
    }
}
