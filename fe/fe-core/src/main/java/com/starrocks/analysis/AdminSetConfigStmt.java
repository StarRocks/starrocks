// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/AdminSetConfigStmt.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.google.common.collect.Maps;
import com.starrocks.sql.ast.AstVisitor;

import java.util.Map;

// admin set frontend config ("key" = "value");
public class AdminSetConfigStmt extends DdlStmt {

    public enum ConfigType {
        FRONTEND,
        BACKEND
    }

    private ConfigType type;
    private Map<String, String> configs;

    private RedirectStatus redirectStatus = RedirectStatus.NO_FORWARD;

    public AdminSetConfigStmt(ConfigType type, Map<String, String> configs) {
        this.type = type;
        this.configs = configs;
        if (this.configs == null) {
            this.configs = Maps.newHashMap();
        }
    }

    public ConfigType getType() {
        return type;
    }

    public Map<String, String> getConfigs() {
        return configs;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return redirectStatus;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAdminSetConfigStatement(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
