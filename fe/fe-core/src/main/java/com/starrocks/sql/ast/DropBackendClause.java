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

import java.util.List;

public class DropBackendClause extends BackendClause {
    private final boolean force;

    public DropBackendClause(List<String> hostPorts) {
        super(hostPorts, NodePosition.ZERO);
        this.force = true;
    }

    public DropBackendClause(List<String> hostPorts, boolean force) {
        this(hostPorts, force, NodePosition.ZERO);
    }

    public DropBackendClause(List<String> hostPorts, boolean force, NodePosition pos) {
        super(hostPorts, pos);
        this.force = force;
    }

    public boolean isForce() {
        return force;
    }
}
