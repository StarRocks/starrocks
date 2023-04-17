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

public class DropPolicyStmt extends DdlStmt {
    private final PolicyType policyType;
    private final PolicyName policyName;
    private final boolean ifExists;
    private final boolean force;

    //Resolved by analyzer
    private Long policyId;

    public DropPolicyStmt(PolicyType policyType, PolicyName policyName, boolean ifExists, boolean force,
                          NodePosition nodePosition) {
        super(nodePosition);
        this.policyType = policyType;
        this.policyName = policyName;
        this.ifExists = ifExists;
        this.force = force;
    }

    public PolicyType getPolicyType() {
        return policyType;
    }

    public PolicyName getPolicyName() {
        return policyName;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public Long getPolicyId() {
        return policyId;
    }

    public void setPolicyId(Long policyId) {
        this.policyId = policyId;
    }

    public boolean isForce() {
        return force;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropPolicyStatement(this, context);
    }
}
