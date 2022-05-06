// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import java.util.ArrayList;
import java.util.List;

import com.starrocks.common.Pair;
import com.starrocks.sql.ast.AstVisitor;


public class ModifyBackendAddressClause extends BackendClause {

    protected String wantToModifyHostPort;
    // Although the FQDN is declared here, 
    // the value of this field may still be an IP, which is currently allowed
    protected String fqdn;

    protected Pair<String, Integer>  wantToModifyHostPortPair;
    
    public ModifyBackendAddressClause(Pair<String, Integer> wPair, String fqdn) {
        super(new ArrayList<String>());
        this.wantToModifyHostPortPair = wPair;
        this.fqdn = fqdn;
    }

    protected ModifyBackendAddressClause(List<String> hostPorts) {
        super(new ArrayList<String>());
    }

    public ModifyBackendAddressClause(String wantToModifyHostPort, String fqdn) {
        super(new ArrayList<String>());
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
        return visitor.visitModifyBackendHostClause(this, context);
    }
}
