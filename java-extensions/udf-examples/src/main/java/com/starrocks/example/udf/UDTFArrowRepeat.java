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

package com.starrocks.example.udf;

import org.apache.arrow.vector.IntVector;

// Vectorized ("input"="arrow") UDTF: for each input row emit two rows {x, x*10}.
// Input is a whole-batch FieldVector; output stays boxed T[][] (one T[] per input row).
public class UDTFArrowRepeat {
    public Integer[][] process(IntVector v) {
        Integer[][] out = new Integer[v.getValueCount()][];
        for (int i = 0; i < v.getValueCount(); i++) {
            if (v.isNull(i)) {
                out[i] = new Integer[0];
            } else {
                int x = v.get(i);
                out[i] = new Integer[] {x, x * 10};
            }
        }
        return out;
    }
}
