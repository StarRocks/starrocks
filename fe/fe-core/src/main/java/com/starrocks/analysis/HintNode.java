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

package com.starrocks.analysis;

import com.starrocks.common.UserException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

public abstract class HintNode implements ParseNode {

    protected Map<String, String> value;

    protected final NodePosition pos;

    protected String hintStr;

    protected HintNode(NodePosition pos, Map<String, String> value, String hintStr) {
        this.pos = pos;
        this.value = value;
        this.hintStr = hintStr;
    }

    public Map<String, String> getValue() {
        return value;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
    }


    @Override
    public String toSql() {
        return "";
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitHintNode(this, context);
    }
}
