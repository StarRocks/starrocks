// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import java.util.ArrayList;
import java.util.List;

import com.starrocks.sql.ast.AstVisitor;


public class ModifyBackendAddressClause extends BackendClause {

    protected String srcHost;
    protected String destHost;
    
    protected ModifyBackendAddressClause(List<String> hostPorts) {
        super(new ArrayList<String>());
    }

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
