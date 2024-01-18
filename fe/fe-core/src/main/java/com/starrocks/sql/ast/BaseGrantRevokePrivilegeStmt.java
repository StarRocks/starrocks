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

import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.ResourcePattern;
import com.starrocks.analysis.TablePattern;
import com.starrocks.authz.authorization.ObjectType;
import com.starrocks.authz.authorization.PEntryObject;
import com.starrocks.authz.authorization.PrivilegeType;
import com.starrocks.common.structure.Pair;
import com.starrocks.mysql.privilege.PrivBitSet;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public class BaseGrantRevokePrivilegeStmt extends DdlStmt {
    protected GrantRevokeClause clause;
    protected GrantRevokePrivilegeObjects objectsUnResolved;
    private boolean isGrantOnAll = false;

    protected String role;
    protected String objectTypeUnResolved;
    protected List<String> privilegeTypeUnResolved;

    // the following fields is set by analyzer for old privilege framework and will be removed after 2.5 released
    private PrivBitSet privBitSet = null;
    private TablePattern tblPattern = null;
    private ResourcePattern resourcePattern = null;

    // the following fields is set by analyzer, for new RBAC privilege framework
    private ObjectType objectType;
    private List<PrivilegeType> privilegeTypes;
    private List<PEntryObject> objectList;

    public BaseGrantRevokePrivilegeStmt(
            List<String> privilegeTypeUnResolved,
            String objectTypeUnResolved,
            GrantRevokeClause clause,
            GrantRevokePrivilegeObjects objects) {
        this(privilegeTypeUnResolved, objectTypeUnResolved, clause, objects, NodePosition.ZERO);
    }

    public BaseGrantRevokePrivilegeStmt(
            List<String> privilegeTypeUnResolved,
            String objectTypeUnResolved,
            GrantRevokeClause clause,
            GrantRevokePrivilegeObjects objectsUnResolved, NodePosition pos) {
        super(pos);
        this.privilegeTypeUnResolved = privilegeTypeUnResolved;
        this.objectTypeUnResolved = objectTypeUnResolved;
        this.clause = clause;
        this.objectsUnResolved = objectsUnResolved;
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
        return objectsUnResolved.getUserPrivilegeObjectList().get(0);
    }

    public List<List<String>> getPrivilegeObjectNameTokensList() {
        return objectsUnResolved.getPrivilegeObjectNameTokensList();
    }

    public List<UserIdentity> getUserPrivilegeObjectList() {
        return objectsUnResolved.getUserPrivilegeObjectList();
    }

    public List<Pair<FunctionName, FunctionArgsDef>> getFunctions() {
        return objectsUnResolved.getFunctions();
    }

    public boolean isGrantOnALL() {
        return isGrantOnAll;
    }

    public void setGrantOnAll() {
        isGrantOnAll = true;
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
        return clause.getUserIdentity();
    }

    public String getObjectTypeUnResolved() {
        return objectTypeUnResolved;
    }

    public List<String> getPrivilegeTypeUnResolved() {
        return privilegeTypeUnResolved;
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

    public ObjectType getObjectType() {
        return objectType;
    }

    public void setObjectType(ObjectType objectType) {
        this.objectType = objectType;
    }

    public List<PrivilegeType> getPrivilegeTypes() {
        return privilegeTypes;
    }

    public void setPrivilegeTypes(List<PrivilegeType> privilegeTypes) {
        this.privilegeTypes = privilegeTypes;
    }

    public List<PEntryObject> getObjectList() {
        return objectList;
    }

    public void setObjectList(List<PEntryObject> objectList) {
        this.objectList = objectList;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitGrantRevokePrivilegeStatement(this, context);
    }
}
