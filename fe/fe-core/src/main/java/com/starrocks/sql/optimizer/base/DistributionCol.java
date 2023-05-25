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

package com.starrocks.sql.optimizer.base;

import java.util.Objects;

public class DistributionCol {

    private final int colId;

    private final boolean nullStrict;

    private final boolean aggStrict;

    public DistributionCol(int colId, boolean nullStrict) {
        this.colId = colId;
        this.nullStrict = nullStrict;
        this.aggStrict = true;
    }

    public DistributionCol(int codId, boolean nullStrict, boolean aggStrict) {
        this.colId = codId;
        this.nullStrict = nullStrict;
        this.aggStrict = aggStrict;
    }

    public int getColId() {
        return colId;
    }

    public boolean isNullStrict() {
        return nullStrict;
    }

    public boolean isAggStrict() {
        return aggStrict;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DistributionCol that = (DistributionCol) o;
        return colId == that.colId && nullStrict == that.nullStrict;
    }

    public DistributionCol getNullRelaxCol() {
        if (!nullStrict) {
            return this;
        }
        return new DistributionCol(colId, false, aggStrict);
    }

    public DistributionCol getNullStrictCol() {
        if (nullStrict) {
            return this;
        }
        return new DistributionCol(colId, true, aggStrict);
    }

    public DistributionCol updateColId(int colId) {
        return new DistributionCol(colId, nullStrict, aggStrict);
    }

    public boolean isSatisfy(DistributionCol requiredCol) {
        if (colId != requiredCol.getColId()) {
            return false;
        }

        return !requiredCol.isNullStrict() || nullStrict;
    }

    @Override
    public int hashCode() {
        return Objects.hash(colId, nullStrict);
    }

    @Override
    public String toString() {
        return colId + "(" + nullStrict + ")";
    }
}
