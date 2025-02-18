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

import com.starrocks.analysis.RedirectStatus;
import com.starrocks.sql.parser.NodePosition;

// admin set frontend config ("key" = "value");
public class AdminSetConfigStmt extends DdlStmt {

    public enum ConfigType {
        FRONTEND,
        BACKEND
    }

    private final ConfigType type;
    private Property config;
    private final boolean persistent;

    public AdminSetConfigStmt(ConfigType type, Property config, boolean persistent, NodePosition pos) {
        super(pos);
        this.type = type;
        this.config = config;
        this.persistent = persistent;
    }

    public ConfigType getType() {
        return type;
    }

    public Property getConfig() {
        return config;
    }

    public boolean isPersistent() {
        return persistent;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAdminSetConfigStatement(this, context);
    }
}
