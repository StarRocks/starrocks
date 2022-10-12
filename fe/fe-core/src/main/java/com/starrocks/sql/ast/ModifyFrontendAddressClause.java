// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.ha.FrontendNodeType;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.FrontendClause;

public class ModifyFrontendAddressClause extends FrontendClause {

    protected String srcHost;
    protected String destHost;
    
    public ModifyFrontendAddressClause(String hostPort, FrontendNodeType role) {
        super(hostPort, role);
    }

    public ModifyFrontendAddressClause(String srcHost, String destHost) {
        super("", FrontendNodeType.UNKNOWN);
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
        return visitor.visitModifyFrontendHostClause(this, context);
    }
}
