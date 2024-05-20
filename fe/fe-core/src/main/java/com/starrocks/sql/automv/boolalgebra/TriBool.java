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

package com.starrocks.sql.automv.boolalgebra;

public enum TriBool {
    TB_FALSE("FALSE"),
    TB_NULL("NULL"),
    TB_TRUE("TRUE");

    private final String sql;

    TriBool(String sql) {
        this.sql = sql;
    }

    public TriBool and(TriBool rhs) {
        return TriBool.values()[Math.min(this.ordinal(), rhs.ordinal())];
    }

    public TriBool or(TriBool rhs) {
        return TriBool.values()[Math.max(this.ordinal(), rhs.ordinal())];
    }

    public TriBool not() {
        return TriBool.values()[2 - this.ordinal()];
    }

    public TriBool eq(TriBool that) {
        if (this == TB_NULL || that == TB_NULL) {
            return TB_NULL;
        } else if (this == that) {
            return TB_TRUE;
        } else {
            return TB_FALSE;
        }
    }

    public TriBool nullSafeEq(TriBool that) {
        if (this == that) {
            return TB_TRUE;
        } else {
            return TB_FALSE;
        }
    }

    public String getSql() {
        return this.sql;
    }
}