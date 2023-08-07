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

import com.starrocks.analysis.ParseNode;
import com.starrocks.sql.parser.NodePosition;

public class Identifier implements ParseNode {
    private final String value;

    private final NodePosition pos;

    public Identifier(String value) {
        this(value, NodePosition.ZERO);
    }

    public Identifier(String value, NodePosition pos) {
        this.pos = pos;
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return null;
    }
}