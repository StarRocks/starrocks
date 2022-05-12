// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import java.util.ArrayList;
import java.util.List;

import com.starrocks.sql.ast.AstVisitor;


public class ModifyBackendAddressClause extends BackendClause {

    protected String toBeModifiedHost;
    // Although the FQDN is declared here, 
    // the value of this field may still be an IP
    protected String fqdn;
    
    protected ModifyBackendAddressClause(List<String> hostPorts) {
        super(new ArrayList<String>());
    }

    public ModifyBackendAddressClause(String toBeModifiedHost, String fqdn) {
        super(new ArrayList<String>());
        this.toBeModifiedHost = toBeModifiedHost;
        this.fqdn = fqdn;
    }

    public String getToBeModifyHost() {
        return toBeModifiedHost;
    }

    public String getFqdn() {
        return fqdn;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitModifyBackendHostClause(this, context);
    }
}
