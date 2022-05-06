// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.common.Pair;

import com.starrocks.ha.FrontendNodeType;
import com.starrocks.sql.ast.AstVisitor;


public class ModifyFrontendAddressClause extends FrontendClause {

    protected String wantToModifyHostPort;
    // Although the FQDN is declared here, 
    // the value of this field may still be an IP, which is currently allowed
    protected String fqdn;

    protected Pair<String, Integer>  wantToModifyHostPortPair;
    
    public ModifyFrontendAddressClause(String hostPort, FrontendNodeType role) {
        super(hostPort, role);
    }

    public ModifyFrontendAddressClause(String wantToModifyHostPort, String fqdn) {
        super(wantToModifyHostPort, FrontendNodeType.UNKNOWN);
        this.wantToModifyHostPort = wantToModifyHostPort;
        this.fqdn = fqdn;
    }
    
    public String getWantToModifyHostPort() {
        return wantToModifyHostPort;
    }

    public void setWantToModifyHostPortPair(Pair<String, Integer> wPair) {
        this.wantToModifyHostPortPair = wPair;
    }

    public Pair<String, Integer> getWantToModifyHostPortPair() {
        return wantToModifyHostPortPair;
    }

    public String getFqdn() {
        return fqdn;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitModifyFrontendHostClause(this, context);
    }
}
