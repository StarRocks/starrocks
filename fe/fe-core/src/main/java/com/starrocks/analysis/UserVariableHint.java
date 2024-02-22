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

import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.UserVariable;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;
import java.util.stream.Collectors;

public class UserVariableHint extends HintNode {

    public static final String SET_USER_VARIABLE = "SET_USER_VARIABLE";

    public static final int LEAST_LEN = 19;

    private Map<String, UserVariable> userVariables;

    public UserVariableHint(NodePosition pos, Map<String, UserVariable> userVariables, String hintStr) {
        super(pos, hintStr);
        this.userVariables = userVariables;
    }

    public Map<String, UserVariable> getUserVariables() {
        return userVariables;
    }

    @Override
    public Map<String, String> getValue() {
        return userVariables.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toSql()));
    }


    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitUserVariableHint(this, context);
    }

}
