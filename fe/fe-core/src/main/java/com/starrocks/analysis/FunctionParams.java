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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/FunctionParams.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.google.common.collect.Lists;
import com.starrocks.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Return value of the grammar production that parses function
 * parameters. These parameters can be for scalar or aggregate functions.
 */
public class FunctionParams implements Writable {
    private boolean isStar;
    private List<Expr> exprs;
    private boolean isDistinct;

    // c'tor for non-star params
    public FunctionParams(boolean isDistinct, List<Expr> exprs) {
        isStar = false;
        this.isDistinct = isDistinct;
        this.exprs = exprs;
    }

    // c'tor for non-star, non-distinct params
    public FunctionParams(List<Expr> exprs) {
        this(false, exprs);
    }

    // c'tor for <agg>(*)
    private FunctionParams() {
        exprs = null;
        isStar = true;
        isDistinct = false;
    }

    public static FunctionParams createStarParam() {
        return new FunctionParams();
    }

    public boolean isStar() {
        return isStar;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    public List<Expr> exprs() {
        return exprs;
    }

    public void setIsDistinct(boolean v) {
        isDistinct = v;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBoolean(isStar);
        out.writeBoolean(isDistinct);
        if (exprs != null) {
            out.writeBoolean(true);
            out.writeInt(exprs.size());
            for (Expr expr : exprs) {
                Expr.writeTo(expr, out);
            }
        } else {
            out.writeBoolean(false);
        }
    }

    public void readFields(DataInput in) throws IOException {
        isStar = in.readBoolean();
        isDistinct = in.readBoolean();
        if (in.readBoolean()) {
            exprs = Lists.newArrayList();
            int size = in.readInt();
            for (int i = 0; i < size; ++i) {
                exprs.add(Expr.readIn(in));
            }
        }
    }

    public static FunctionParams read(DataInput in) throws IOException {
        FunctionParams params = new FunctionParams();
        params.readFields(in);
        return params;
    }

    @Override
    public int hashCode() {
        int result = 31 * Boolean.hashCode(isStar) + Boolean.hashCode(isDistinct);
        if (exprs != null) {
            for (Expr expr : exprs) {
                result = 31 * result + Objects.hashCode(expr);
            }
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof FunctionParams)) {
            return false;
        }

        FunctionParams that = (FunctionParams) obj;
        return isStar == that.isStar && isDistinct == that.isDistinct && Objects.equals(exprs, that.exprs);
    }
}
