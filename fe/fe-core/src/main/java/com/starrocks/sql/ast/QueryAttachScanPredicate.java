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

import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SlotRef;
import com.starrocks.sql.parser.NodePosition;

/**
 * Created by liujing on 2024/6/28.
 */
public class QueryAttachScanPredicate implements ParseNode {

    private final SlotRef[] attachCompareExprs;
    private final LiteralExpr[] attachValueExprs;

    private final NodePosition pos;

    public QueryAttachScanPredicate(SlotRef[] attachCompareExprs,
                                    LiteralExpr[] attachValueExprs) {
        this(attachCompareExprs, attachValueExprs, NodePosition.ZERO);
    }

    public QueryAttachScanPredicate(
            SlotRef[] attachCompareExprs,
            LiteralExpr[] attachValueExprs,
            NodePosition pos) {
        this.attachCompareExprs = attachCompareExprs;
        this.attachValueExprs = attachValueExprs;
        this.pos = pos;

    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    public SlotRef[] getAttachCompareExprs() {
        return attachCompareExprs;
    }

    public LiteralExpr[] getAttachValueExprs() {
        return attachValueExprs;
    }
}
