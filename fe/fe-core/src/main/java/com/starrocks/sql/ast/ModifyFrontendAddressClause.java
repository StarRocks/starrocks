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

import com.starrocks.ha.FrontendNodeType;
import com.starrocks.sql.parser.NodePosition;

public class ModifyFrontendAddressClause extends FrontendClause {

    protected String srcHost;
    protected String destHost;

    public ModifyFrontendAddressClause(String hostPort, FrontendNodeType role) {
        super(hostPort, role, NodePosition.ZERO);
    }

    public ModifyFrontendAddressClause(String srcHost, String destHost) {
        this(srcHost, destHost, NodePosition.ZERO);
    }

    public ModifyFrontendAddressClause(String srcHost, String destHost, NodePosition pos) {
        super("", FrontendNodeType.UNKNOWN, pos);
        this.srcHost = srcHost;
        this.destHost = destHost;
    }

    public String getSrcHost() {
        return srcHost;
    }

    public String getDestHost() {
        return destHost;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitModifyFrontendHostClause(this, context);
    }
}
