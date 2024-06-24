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
// limitations under the License
package com.starrocks.analysis;

import com.google.api.client.util.Lists;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class Exprs {
    private final List<Expr> exprs;

    public Exprs() {
        exprs = Lists.newArrayList();
    }

    public Exprs(List<Expr> exprs) {
        this.exprs = exprs;
    }

    public Exprs(Expr expr) {
        this.exprs = ImmutableList.of(expr);
    }

    public static Exprs of(List<Expr> exprs) {
        return new Exprs(exprs);
    }

    public static Exprs of(Expr expr) {
       return new Exprs(expr);
    }

    public void add(Exprs exprs) {
        this.exprs.addAll(exprs.exprs);
    }

    public List<Expr> getExprs() {
        return exprs;
    }

    public int size() {
        if (exprs == null) {
            return 0;
        }
        return exprs.size();
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public Expr get(int i) {
        return exprs.get(i);
    }
}
