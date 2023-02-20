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
import com.starrocks.common.AnalysisException;

import java.util.List;
import java.util.Map;

// ADMIN CHECK TABLET (id1, id2, ...) PROPERTIES ("type" = "check_consistency");
public class AdminCheckTabletsStmt extends DdlStmt {

    private final List<Long> tabletIds;
    private final Map<String, String> properties;
    private CheckType type;

    public void setType(CheckType type) {
        this.type = type;
    }

    public enum CheckType {
        CONSISTENCY; // check the consistency of replicas of tablet

        public static CheckType getTypeFromString(String str) throws AnalysisException {
            try {
                return CheckType.valueOf(str.toUpperCase());
            } catch (Exception e) {
                throw new AnalysisException("Unknown check type: " + str);
            }
        }
    }

    public AdminCheckTabletsStmt(List<Long> tabletIds, Map<String, String> properties) {
        this.tabletIds = tabletIds;
        this.properties = properties;
    }

    public List<Long> getTabletIds() {
        return tabletIds;
    }

    public CheckType getType() {
        return type;
    }

    public Map<String, String> getProperties() {
        return properties;
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