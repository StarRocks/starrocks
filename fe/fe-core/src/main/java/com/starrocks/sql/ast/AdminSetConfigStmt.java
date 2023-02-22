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

import com.google.common.collect.Maps;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

// admin set frontend config ("key" = "value");
public class AdminSetConfigStmt extends DdlStmt {

    public enum ConfigType {
        FRONTEND,
        BACKEND
    }

    private final ConfigType type;
    private Map<String, String> configs;

    public AdminSetConfigStmt(ConfigType type, Map<String, String> configs) {
        this(type, configs, NodePosition.ZERO);
    }

    public AdminSetConfigStmt(ConfigType type, Map<String, String> configs, NodePosition pos) {
        super(pos);
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
        return RedirectStatus.NO_FORWARD;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAdminSetConfigStatement(this, context);
    }
}
