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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/CaseWhenClause.java

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

import com.starrocks.sql.parser.NodePosition;

/**
 * captures info of a single WHEN expr THEN expr clause.
 */
public class CaseWhenClause implements ParseNode {
    private final Expr whenExpr;
    private final Expr thenExpr;

    private final NodePosition pos;

    public CaseWhenClause(Expr whenExpr, Expr thenExpr) {
        this(whenExpr, thenExpr, NodePosition.ZERO);
    }

    public CaseWhenClause(Expr whenExpr, Expr thenExpr, NodePosition pos) {
        this.pos = pos;
        this.whenExpr = whenExpr;
        this.thenExpr = thenExpr;
    }

    public Expr getWhenExpr() {
        return whenExpr;
    }

    public Expr getThenExpr() {
        return thenExpr;
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }
}
