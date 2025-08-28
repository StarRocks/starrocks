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

package com.starrocks.sql.ast;

import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

public abstract class HintNode implements Comparable<HintNode>, ParseNode {
    public static final String HINT_JOIN_BUCKET = "BUCKET";
    public static final String HINT_JOIN_SHUFFLE = "SHUFFLE";
    public static final String HINT_JOIN_COLOCATE = "COLOCATE";
    public static final String HINT_JOIN_BROADCAST = "BROADCAST";

    public static final String HINT_JOIN_SKEW = "SKEW";
    public static final String HINT_JOIN_UNREORDER = "UNREORDER";

    public static final String HINT_ANALYTIC_SORT = "sort";
    public static final String HINT_ANALYTIC_HASH = "hash";
    public static final String HINT_ANALYTIC_SKEW = "skewed";

    protected final NodePosition pos;

    protected final String hintStr;

    protected HintNode(NodePosition pos, String hintStr) {
        this.pos = pos;
        this.hintStr = hintStr;
    }

    public Scope getScope() {
        return Scope.QUERY;
    }

    public abstract Map<String, String> getValue();

    @Override
    public String toSql() {
        return hintStr;
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    @Override
    public int compareTo(HintNode o) {
        return hintStr.compareTo(o.hintStr);
    }

    public enum Scope {
        // the entire query
        QUERY,
        // part of a query. Like hint in select * from (select hint from tbl),
        // we may want the hint only takes effect in the subquery in the future.
        CLAUSE
    }
}
