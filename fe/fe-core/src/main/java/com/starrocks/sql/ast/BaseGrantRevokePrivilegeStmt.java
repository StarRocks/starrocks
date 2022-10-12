// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.ResourcePattern;
import com.starrocks.analysis.TablePattern;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.mysql.privilege.PrivBitSet;
import com.starrocks.privilege.ActionSet;
import com.starrocks.privilege.PEntryObject;

import java.util.List;

public class BaseGrantRevokePrivilegeStmt extends DdlStmt {
    protected String role;
    protected UserIdentity userIdentity;
    protected String privType;
    protected List<String> privList;

    private List<String> privilegeObjectNameTokenList = null;
    private TablePattern tblPattern = null;
    private ResourcePattern resourcePattern = null;
    private UserIdentity userPrivilegeObject = null;
    // the following fields is set by analyzer
    private PrivBitSet privBitSet = null;
    // the following fields is set by analyzer, for new RBAC privilege framework
    private boolean withGrantOption = false;
    private short typeId;
    private ActionSet actionList;
    private PEntryObject object;

    public BaseGrantRevokePrivilegeStmt(List<String> privList, String privType, UserIdentity userIdentity) {
        this.privList = privList;
        this.privType = privType;
        this.userIdentity = userIdentity;
        this.role = null;
    }

    public BaseGrantRevokePrivilegeStmt(List<String> privList, String privType, String roleName) {
        this.privList = privList;
        this.privType = privType;
        this.userIdentity = null;
        this.role = roleName;
    }

    public void setAnalysedTable(PrivBitSet privBitSet, TablePattern tablePattern) {
        this.privBitSet = privBitSet;
        this.tblPattern = tablePattern;
    }

    public void setAnalysedResource(PrivBitSet privBitSet, ResourcePattern resourcePattern) {
        this.privBitSet = privBitSet;
        this.resourcePattern = resourcePattern;
    }

    public void setUserPrivilegeObject(UserIdentity userPrivilegeObject) {
        this.userPrivilegeObject = userPrivilegeObject;
    }

    public void setPrivilegeObjectNameTokenList(List<String> privilegeObjectNameTokenList) {
        this.privilegeObjectNameTokenList = privilegeObjectNameTokenList;
    }

    public void setPrivBitSet(PrivBitSet privBitSet) {
        this.privBitSet = privBitSet;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public String getRole() {
        return role;
    }

    public UserIdentity getUserIdentity() {
        return userIdentity;
    }

    public String getPrivType() {
        return privType;
    }

    public List<String> getPrivList() {
        return privList;
    }

    public TablePattern getTblPattern() {
        return tblPattern;
    }

    public ResourcePattern getResourcePattern() {
        return resourcePattern;
    }

    public List<String> getPrivilegeObjectNameTokenList() {
        return privilegeObjectNameTokenList;
    }

    public UserIdentity getUserPrivilegeObject() {
        return userPrivilegeObject;
    }

    public PrivBitSet getPrivBitSet() {
        return privBitSet;
    }

    public boolean isWithGrantOption() {
        return withGrantOption;
    }

    public short getTypeId() {
        return typeId;
    }

    public void setTypeId(short typeId) {
        this.typeId = typeId;
    }

    public ActionSet getActionList() {
        return actionList;
    }

    public void setActionList(ActionSet actionList) {
        this.actionList = actionList;
    }

    public PEntryObject getObject() {
        return object;
    }

    public void setObject(PEntryObject object) {
        this.object = object;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitGrantRevokePrivilegeStatement(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
