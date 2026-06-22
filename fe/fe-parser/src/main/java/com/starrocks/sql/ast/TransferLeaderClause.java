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

// ALTER SYSTEM TRANSFER LEADER TO "host:edit_log_port" [FORCE]
//
// Gracefully hand the FE leader role to the target electable frontend by calling
// BDBJE transferMaster in-process (instead of shelling out to DbGroupAdmin). FORCE
// supersedes a master transfer that is already in progress; without FORCE the
// statement fails if another transfer is in progress.
public class TransferLeaderClause extends FrontendClause {

    private final boolean force;

    public TransferLeaderClause(String hostPort, boolean force) {
        this(hostPort, force, NodePosition.ZERO);
    }

    public TransferLeaderClause(String hostPort, boolean force, NodePosition pos) {
        super(hostPort, pos);
        this.force = force;
    }

    public boolean isForce() {
        return force;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitTransferLeaderClause(this, context);
    }
}
