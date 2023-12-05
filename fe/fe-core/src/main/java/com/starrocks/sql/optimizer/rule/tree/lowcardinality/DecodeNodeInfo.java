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

package com.starrocks.sql.optimizer.rule.tree.lowcardinality;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefSet;

/*
 * For record the string columns on operator
 */
class DecodeNodeInfo {
    public static final DecodeNodeInfo EMPTY = new DecodeNodeInfo();
    OptExpression parent = null;

    // operator's input string columns
    ColumnRefSet inputStringColumns = new ColumnRefSet();

    // operator's required decode string columns
    ColumnRefSet decodeStringColumns = new ColumnRefSet();

    // operator's output string columns
    ColumnRefSet outputStringColumns = new ColumnRefSet();

    public DecodeNodeInfo createOutputInfo() {
        if (this.outputStringColumns.isEmpty()) {
            return EMPTY;
        }

        DecodeNodeInfo info = new DecodeNodeInfo();
        info.inputStringColumns.union(this.outputStringColumns);
        info.outputStringColumns.union(this.outputStringColumns);
        return info;
    }

    public DecodeNodeInfo createDecodeInfo() {
        if (this.outputStringColumns.isEmpty()) {
            return EMPTY;
        }

        DecodeNodeInfo info = new DecodeNodeInfo();
        info.decodeStringColumns.union(this.outputStringColumns);
        return info;
    }

    public boolean isEmpty() {
        return this.outputStringColumns.isEmpty() && this.inputStringColumns.isEmpty() &&
                this.decodeStringColumns.isEmpty();
    }

    public void addChildInfo(DecodeNodeInfo other) {
        this.outputStringColumns.union(other.outputStringColumns);
    }

    @Override
    public String toString() {
        return "input[" + inputStringColumns + "], " + "decode[" + decodeStringColumns + "], " + "output[" +
                outputStringColumns + ']';
    }
}
