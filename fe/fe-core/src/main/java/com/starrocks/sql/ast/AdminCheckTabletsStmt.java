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

import java.util.List;

// ADMIN CHECK TABLET (id1, id2, ...) PROPERTIES ("type" = "check_consistency");
public class AdminCheckTabletsStmt extends DdlStmt {

    private final List<Long> tabletIds;
    private final Property property;
    private CheckType type;

    public void setType(CheckType type) {
        this.type = type;
    }

    public enum CheckType {
        CONSISTENCY // check the consistency of replicas of tablet
    }

    public AdminCheckTabletsStmt(List<Long> tabletIds, Property property, NodePosition pos) {
        super(pos);
        this.tabletIds = tabletIds;
        this.property = property;
    }

    public List<Long> getTabletIds() {
        return tabletIds;
    }

    public CheckType getType() {
        return type;
    }

    public Property getProperty() {
        return property;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAdminCheckTabletsStatement(this, context);
    }
}