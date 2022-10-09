// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.UserIdentity;

import java.util.List;

public class GrantRevokePrivilegeObjects implements ParseNode {
    private List<List<String>> privilegeObjectNameTokensList = null;
    private List<UserIdentity> userPrivilegeObjectList = null;
    // grant create_table on all databases to userx
    // ==> allTypeList: ["databases"], restrictType: null, restrictName: null
    // grant select on all tables in database db1 to userx
    // ==> allTypeList: ["tables"], restrictType: database, restrictName: db1
    // grant select on all tables in all databases to userx
    // ==> allTypeList: ["tables", "databases"], restrictType: null, restrictName: null
    private List<String> allTypeList = null;
    private String restrictType = null;
    private String restrictName = null;

    public GrantRevokePrivilegeObjects() {}

    public void setPrivilegeObjectNameTokensList(List<List<String>> privilegeObjectNameTokensList) {
        this.privilegeObjectNameTokensList = privilegeObjectNameTokensList;
    }

    public void setUserPrivilegeObjectList(List<UserIdentity> userPrivilegeObjectList) {
        this.userPrivilegeObjectList = userPrivilegeObjectList;
    }

    public void setAll(List<String> allTypeList, String restrictType, String restrictName) {
        this.allTypeList = allTypeList;
        this.restrictType = restrictType;
        this.restrictName = restrictName;
    }

    public List<List<String>> getPrivilegeObjectNameTokensList() {
        return privilegeObjectNameTokensList;
    }

    public List<UserIdentity> getUserPrivilegeObjectList() {
        return userPrivilegeObjectList;
    }

    public List<String> getAllTypeList() {
        return allTypeList;
    }

    public String getRestrictType() {
        return restrictType;
    }

    public String getRestrictName() {
        return restrictName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return null;
    }
}
