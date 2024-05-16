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

import java.util.ArrayList;
import java.util.Map;

/**
 * For:
 * <p>
 * 1. ALTER SYSTEM MODIFY BACKEND HOST fqdn_or_ip to fqdn_or_ip
 * <p>
 * 2. ALTER SYSTEM MODIFY BACKEND host:port SET ("key" = "value")
 */
public class ModifyBackendClause extends BackendClause {
    protected String srcHost;
    protected String destHost;

    /**
     * When null, modify backend set properties.
     * When not null, modify backend host, i.e. for FQDN feature
     */
    private String backendHostPort;
    private Map<String, String> properties;

    public ModifyBackendClause(String srcHost, String destHost) {
        this(srcHost, destHost, NodePosition.ZERO);
    }

    public ModifyBackendClause(String backendHostPort, Map<String, String> properties) {
        this(backendHostPort, properties, NodePosition.ZERO);
    }

    public ModifyBackendClause(String srcHost, String destHost, NodePosition pos) {
        super(new ArrayList<>(), pos);
        this.srcHost = srcHost;
        this.destHost = destHost;
    }

    public ModifyBackendClause(String backendHostPort, Map<String, String> properties, NodePosition pos) {
        super(new ArrayList<>(), pos);
        this.backendHostPort = backendHostPort;
        this.properties = properties;
    }

    public String getSrcHost() {
        return srcHost;
    }

    public String getDestHost() {
        return destHost;
    }

    public String getBackendHostPort() {
        return backendHostPort;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitModifyBackendClause(this, context);
    }
}
