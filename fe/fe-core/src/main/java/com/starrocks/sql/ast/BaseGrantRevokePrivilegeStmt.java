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

import com.starrocks.analysis.ResourcePattern;
import com.starrocks.analysis.TablePattern;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.mysql.privilege.PrivBitSet;
import com.starrocks.privilege.ActionSet;
import com.starrocks.privilege.ObjectType;
import com.starrocks.privilege.PEntryObject;

import java.util.List;

public class BaseGrantRevokePrivilegeStmt extends DdlStmt {
    protected GrantRevokeClause clause;
    protected GrantRevokePrivilegeObjects objects;

    protected String role;
    protected String privType;
    protected List<String> privList;

    // the following fields is set by analyzer for old privilege framework and will be removed after 2.5 released
    private PrivBitSet privBitSet = null;
    private TablePattern tblPattern = null;
    private ResourcePattern resourcePattern = null;

    // the following fields is set by analyzer, for new RBAC privilege framework
    private ObjectType objectType;
    private ActionSet actionList;
    private List<PEntryObject> objectList;

    public BaseGrantRevokePrivilegeStmt(
            List<String> privList,
            String privType,
            GrantRevokeClause clause,
            GrantRevokePrivilegeObjects objects) {
        this.privList = privList;
        this.privType = privType;
        this.clause = clause;
        this.objects = objects;
        this.role = clause.getRoleName();
    }

    public void setAnalysedTable(PrivBitSet privBitSet, TablePattern tablePattern) {
        this.privBitSet = privBitSet;
        this.tblPattern = tablePattern;
    }

    public void setAnalysedResource(PrivBitSet privBitSet, ResourcePattern resourcePattern) {
        this.privBitSet = privBitSet;
        this.resourcePattern = resourcePattern;
    }

    /**
     * old privilege framework only support grant/revoke on one single object
     */
    public UserIdentity getUserPrivilegeObject() {
        return objects.getUserPrivilegeObjectList().get(0);
    }

    public List<List<String>> getPrivilegeObjectNameTokensList() {
        return objects.getPrivilegeObjectNameTokensList();
    }

    public List<UserIdentity> getUserPrivilegeObjectList() {
        return objects.getUserPrivilegeObjectList();
    }

    public FunctionArgsDef getFunctionArgsDef() {
        return objects.getFunctionArgsDef();
    }

    public String getFunctionName() {
        return objects.getFunctionName();
    }
    public List<String> getAllTypeList() {
        return objects.getAllTypeList();
    }

    public String getRestrictType() {
        return objects.getRestrictType();
    }

    public String getRestrictName() {
        return objects.getRestrictName();
    }

    public void setPrivBitSet(PrivBitSet privBitSet) {
        this.privBitSet = privBitSet;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public void setPrivType(String privType) {
        this.privType = privType;
    }

    public String getRole() {
        return role;
    }

    public UserIdentity getUserIdentity() {
        return clause.getUserIdentity();
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

    public PrivBitSet getPrivBitSet() {
        return privBitSet;
    }

    public boolean isWithGrantOption() {
        return clause.isWithGrantOption();
    }

    public short getTypeId() {
        return (short) objectType.getId();
    }

    public ObjectType getObjectType() {
        return objectType;
    }

    public void setObjectType(ObjectType objectType) {
        this.objectType = objectType;
    }

    public ActionSet getActionList() {
        return actionList;
    }

    public void setActionList(ActionSet actionList) {
        this.actionList = actionList;
    }

    public List<PEntryObject> getObjectList() {
        return objectList;
    }

    public void setObjectList(List<PEntryObject> objectList) {
        this.objectList = objectList;
    }

    public boolean hasPrivilegeObject() {
        return this.objects != null;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitGrantRevokePrivilegeStatement(this, context);
    }
}
