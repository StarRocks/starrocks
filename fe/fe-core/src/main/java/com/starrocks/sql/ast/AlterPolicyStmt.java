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

import com.starrocks.analysis.Expr;
import com.starrocks.sql.parser.NodePosition;

public class AlterPolicyStmt extends DdlStmt {

    private final PolicyType policyType;
    private final boolean ifExists;
    private final PolicyName policyName;
    private final AlterPolicyClause alterPolicyClause;

    //Resolved by analyzer
    private Long policyId;

    public AlterPolicyStmt(PolicyType policyType, PolicyName policyName, boolean ifExists, AlterPolicyClause alterPolicyClause,
                           NodePosition pos) {
        super(pos);
        this.policyType = policyType;
        this.policyName = policyName;
        this.ifExists = ifExists;
        this.alterPolicyClause = alterPolicyClause;
    }

    public PolicyType getPolicyType() {
        return policyType;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public PolicyName getPolicyName() {
        return policyName;
    }

    public AlterPolicyClause getAlterPolicyClause() {
        return alterPolicyClause;
    }

    public abstract static class AlterPolicyClause {
    }

    public static class PolicySetBody extends AlterPolicyClause {
        private final Expr policyBody;

        public PolicySetBody(Expr policyBody) {
            this.policyBody = policyBody;
        }

        public Expr getPolicyBody() {
            return policyBody;
        }
    }

    public static class PolicyRename extends AlterPolicyClause {
        private final String newPolicyName;

        public PolicyRename(String newPolicyName) {
            this.newPolicyName = newPolicyName;
        }

        public String getNewPolicyName() {
            return newPolicyName;
        }
    }

    public static class PolicySetComment extends AlterPolicyClause {
        private final String comment;

        public PolicySetComment(String comment) {
            this.comment = comment;
        }

        public String getComment() {
            return comment;
        }
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterPolicyStatement(this, context);
    }
}


