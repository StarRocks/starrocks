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


package com.starrocks.sql.common;

import java.util.List;

/**
 * Generate data by the control of `totalCount` & `multiplier`.
 * eg, data: [a, b, c], totalCount: 6, multiplier: 2, the generator will output each time:
 * a,
 * a,
 * b,
 * b,
 * c,
 * c
 *
 * @param <T>
 */
public class Generator<T> {
    private List<T> data;
    private int multiplier;
    private int totalCount;
    private int iTotal;

    public Generator(List<T> data, int totalCount, int multiplier) {
        this.data = data;
        assert (data.size() > 0);
        assert (totalCount % multiplier == 0);
        assert (totalCount % data.size() == 0);
        this.totalCount = totalCount;
        this.multiplier = multiplier;
        this.iTotal = 0;
    }

    public boolean hasNext() {
        return iTotal < totalCount;
    }

    public T next() {
        assert (iTotal < totalCount);
        T ans = data.get((iTotal / multiplier) % data.size());
        iTotal++;
        return ans;
    }
}
