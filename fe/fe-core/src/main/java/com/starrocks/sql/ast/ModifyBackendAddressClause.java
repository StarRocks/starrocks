// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import java.util.ArrayList;

public class ModifyBackendAddressClause extends BackendClause {
    protected String srcHost;
    protected String destHost;

    public ModifyBackendAddressClause(String srcHost, String destHost) {
        super(new ArrayList<String>());
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
        return visitor.visitModifyBackendHostClause(this, context);
    }
}
