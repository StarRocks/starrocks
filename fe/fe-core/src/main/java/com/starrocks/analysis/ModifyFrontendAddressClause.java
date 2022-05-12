// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.ha.FrontendNodeType;
import com.starrocks.sql.ast.AstVisitor;


public class ModifyFrontendAddressClause extends FrontendClause {

    protected String toBeModifiedHost;
    // Although the FQDN is declared here, 
    // the value of this field may still be an IP
    protected String fqdn;
    
    public ModifyFrontendAddressClause(String hostPort, FrontendNodeType role) {
        super(hostPort, role);
    }

    public ModifyFrontendAddressClause(String toBeModifiedHost, String fqdn) {
        super("", FrontendNodeType.UNKNOWN);
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
        return visitor.visitModifyFrontendHostClause(this, context);
    }
}
