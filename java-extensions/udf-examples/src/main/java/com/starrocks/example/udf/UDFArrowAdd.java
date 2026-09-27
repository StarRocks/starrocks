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

// Vectorized ("input"="arrow") scalar UDF: element-wise a + b + 1 over the whole batch.
// Allocate the result from the framework-managed allocator via arg.getAllocator().
public class UDFArrowAdd {
    public IntVector evaluate(IntVector a, IntVector b) {
        IntVector out = new IntVector("result", a.getAllocator());
        int n = a.getValueCount();
        out.allocateNew(n);
        for (int i = 0; i < n; i++) {
            if (a.isNull(i) || b.isNull(i)) {
                out.setNull(i);
            } else {
                out.set(i, a.get(i) + b.get(i) + 1);
            }
        }
        out.setValueCount(n);
        return out;
    }
}
