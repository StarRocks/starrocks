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

import java.util.Map;

public class CreateRepositoryStmt extends DdlStmt {
    private final boolean isReadOnly;
    private final String name;
    private final String brokerName;
    private final String location;
    private final Map<String, String> properties;
    private final boolean hasBroker;

    public CreateRepositoryStmt(boolean isReadOnly, String name, String brokerName, String location,
                                Map<String, String> properties) {
        this(isReadOnly, name, brokerName, location, properties, NodePosition.ZERO);
    }

    public CreateRepositoryStmt(boolean isReadOnly, String name, String brokerName, String location,
                                Map<String, String> properties, NodePosition pos) {
        super(pos);
        this.isReadOnly = isReadOnly;
        this.name = name;
        this.brokerName = brokerName;
        this.location = location;
        this.properties = properties;
        if (brokerName == null) {
            hasBroker = false;
        } else {
            hasBroker = true;
        }
    }

    public boolean isReadOnly() {
        return isReadOnly;
    }

    public String getName() {
        return name;
    }

    public boolean hasBroker() {
        return hasBroker;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public String getLocation() {
        return location;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateRepositoryStatement(this, context);
    }
}
