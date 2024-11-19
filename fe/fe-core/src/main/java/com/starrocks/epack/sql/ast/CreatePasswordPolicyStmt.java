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
package com.starrocks.epack.sql.ast;

import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

public class CreatePasswordPolicyStmt extends DdlStmt {
    private final String policyName;
    private final String comment;
    private final Map<String, String> properties;

    public CreatePasswordPolicyStmt(String policyName, String comment, Map<String, String> properties, NodePosition pos) {
        super(pos);
        this.policyName = policyName;
        this.comment = comment;
        this.properties = properties;
    }

    public String getPolicyName() {
        return policyName;
    }

    public String getComment() {
        return comment;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        if (visitor instanceof AstVisitorEPack) {
            return ((AstVisitorEPack<R, C>) visitor).visitCreatePasswordPolicyStatement(this, context);
        } else {
            return null;
        }
    }
}
