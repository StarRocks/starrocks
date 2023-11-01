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

public final class DistributionCol {

    private final int colId;

    // used to record the null value in this col is distributed like normal value.
    // true means the distribution of null value is same with normal value.
    // false means null value may be randomly distributed.
    // Given the following example：
    //      select tblA.c1, tblB.c2,tblC.c3 from tblA
    //          left join tblB on tblA.c1 = tblB.c1
    //          left join tblC on tblB.c1 = tblC.c1
    // The data distribution after the first join can be described as tblA.c1(nullStrict = true) or
    // tblB.c1(nullStrict = false).
    private final boolean nullStrict;


    // special process for scenes:
    // select tblC.c1, t.c1, t.max from tblC left join (
    //      select tblB.c1, max(tbl.c2) max from tblA left join tblB on tblA.c1 = tblB.c1 group by tblB.c1
    // ) t on tblC.c1 = t.c1
    // The presence of non-null values in tblB.c1 satisfies the requirements for global aggregation based
    // on the group by key tblB.c1. However, the null values do not meet the requirements. To compute the
    // final result of the group by tblB.c1, the data needs to be exchanged based on tblB.c1 before further
    // processing. In the second left join, when using tblC as the reference, the redundant null rows of
    // tblB.c1 that are present in table t will not affect the final join result. Therefore, during the
    // aggregation phase, it is possible to generate an "incorrect" aggregation result using local aggregation.
    // However, this "incorrect" aggregation result will still produce the final correct result when left joined
    // with tblC.
    // When the aggStrict is false, it just means the global agg just require its child output distribution can
    // be null relax.
    // Apart from this specific use case, in all other scenarios, we can consider it as non-existent.
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
        return new DistributionCol(colId, false, aggStrict);
    }

    public DistributionCol getNullStrictCol() {
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
    public DistributionCol clone() {
        return new DistributionCol(this.colId, this.nullStrict, this.aggStrict);
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
