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

package com.starrocks.sql.automv.column;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.TableName;
import com.starrocks.sql.automv.pn.Apply;
import com.starrocks.sql.automv.pn.Op;
import com.starrocks.sql.automv.pn.OpUtil;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.type.Type;

import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;

// GenericColumn is basic class of output column of PlanPiece
public abstract class GenericColumn {

    private transient String str = null;
    private transient GenericColumn norm = null;

    public static GenericColumn original(TableName fqTableName, Column column) {
        return new OriginalColumn(fqTableName, column);
    }

    public static GenericColumn derived(Op expr) {
        return new DerivedColumn(expr);
    }

    public final GenericColumn getNorm() {
        return Objects.requireNonNull(norm);
    }

    public final void setNorm(GenericColumn norm) {
        this.norm = Objects.requireNonNull(norm);
    }

    public final String toString() {
        if (str == null) {
            str = toStringImpl();
        }
        return str;
    }

    public abstract GenericColumn clone();

    protected abstract String toStringImpl();

    public boolean isOriginal() {
        return this instanceof OriginalColumn;
    }

    public boolean isDerived() {
        return this instanceof DerivedColumn;
    }

    public abstract String getFQName();

    @SuppressWarnings("unchecked")
    public <T extends GenericColumn> T cast() {
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public <T extends GenericColumn> Optional<T> cast(Class<T> cls) {
        if (this.getClass().equals(cls)) {
            return Optional.of((T) this);
        } else {
            return Optional.empty();
        }
    }

    @SuppressWarnings("unchecked")
    public <T extends GenericColumn> T mustCast(Class<T> cls) {
        Preconditions.checkArgument(this.getClass().equals(cls));
        return (T) this;
    }

    public Op getOp() {
        Preconditions.checkArgument(this.isDerived());
        DerivedColumn dColumn = this.cast();
        return dColumn.getExpr().getOp();
    }

    public Op getNormalizedOp() {
        if (this.isOriginal()) {
            OriginalColumn oColumn = this.cast();
            return Apply.variable(oColumn.getType(), oColumn.getNorm().getFQName());
        } else {
            Preconditions.checkArgument(this.isDerived());
            DerivedColumn dColumn = this.cast();
            GenericColumn normColumn = dColumn.getNorm();
            if (normColumn.isOriginal()) {
                return normColumn.getNormalizedOp();
            } else {
                return normColumn.mustCast(DerivedColumn.class).getExpr().getOp();
            }
        }
    }

    public abstract Type getType();

    @Nullable
    public String getColumnName() {
        return null;
    }

    public Optional<ColumnRefSet> getUsedColumns() {
        if (isDerived()) {
            DerivedColumn derivedColumn = (DerivedColumn) this;
            return Optional.of(derivedColumn.getExpr().getOp().getIds());
        } else {
            return Optional.empty();
        }
    }

    public GenericColumn unfold(TieredMap<Integer, GenericColumn> underlyingColumns) {
        if (this.isOriginal()) {
            Preconditions.checkState(this.getNorm() != null);
            return this;
        } else {
            DerivedColumn derivedColumn = this.cast();
            return OpUtil.unfoldDerivedColumn(derivedColumn, underlyingColumns);
        }
    }
}
