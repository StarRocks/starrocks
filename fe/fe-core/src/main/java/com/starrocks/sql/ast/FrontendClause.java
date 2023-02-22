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

import com.starrocks.alter.AlterOpType;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.sql.parser.NodePosition;
import org.apache.commons.lang.NotImplementedException;

import java.util.Map;

public class FrontendClause extends AlterClause {
    protected String hostPort;
    protected String host;
    protected int port;
    protected FrontendNodeType role;

    protected FrontendClause(String hostPort, FrontendNodeType role, NodePosition pos) {
        super(AlterOpType.ALTER_OTHER, pos);
        this.hostPort = hostPort;
        this.role = role;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getHostPort() {
        return hostPort;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public Map<String, String> getProperties() {
        throw new NotImplementedException();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitFrontendClause(this, context);
    }
}
