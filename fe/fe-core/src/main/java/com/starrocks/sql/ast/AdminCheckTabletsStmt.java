// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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