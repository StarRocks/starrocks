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
    //UnResolved AST used in syntax grantPrivWithType
    private List<List<String>> privilegeObjectNameTokensList;

    //UnResolved AST used in syntax grantImpersonate
    private List<UserIdentity> userPrivilegeObjectList;

    //UnResolved AST used in syntax grantOnAll
    private boolean isAllDB;
    private String dbName;

    //UnResolved AST used in syntax grantPrivWithFunc
    private FunctionArgsDef functionArgsDef;
    private String functionName;

    public List<List<String>> getPrivilegeObjectNameTokensList() {
        return privilegeObjectNameTokensList;
    }

    public void setPrivilegeObjectNameTokensList(List<List<String>> privilegeObjectNameTokensList) {
        this.privilegeObjectNameTokensList = privilegeObjectNameTokensList;
    }

    public List<UserIdentity> getUserPrivilegeObjectList() {
        return userPrivilegeObjectList;
    }

    public void setUserPrivilegeObjectList(List<UserIdentity> userPrivilegeObjectList) {
        this.userPrivilegeObjectList = userPrivilegeObjectList;
    }

    public boolean isAllDB() {
        return isAllDB;
    }

    public String getDbName() {
        return dbName;
    }

    public void setAllPrivilegeObject(boolean isAllDB, String dbName) {
        this.isAllDB = isAllDB;
        this.dbName = dbName;
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
}
