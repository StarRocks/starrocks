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

    private FunctionArgsDef functionArgsDef = null;

    private String functionName = null;
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

    public String getFunctionName() {
        return functionName;
    }

    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }

    public FunctionArgsDef getFunctionArgsDef() {
        return functionArgsDef;
    }

    public void setFunctionArgsDef(FunctionArgsDef functionArgsDef) {
        this.functionArgsDef = functionArgsDef;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return null;
    }
}
