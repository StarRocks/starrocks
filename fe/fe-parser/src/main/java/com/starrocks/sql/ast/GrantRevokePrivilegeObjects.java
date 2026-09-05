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

import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public class GrantRevokePrivilegeObjects implements ParseNode {
    //UnResolved AST used in syntax grantPrivWithType
    private List<List<String>> privilegeObjectNameTokensList;

    //UnResolved AST used in syntax grantImpersonate
    private List<UserRef> userPrivilegeObjectList;

    //UnResolved AST used in syntax grantPrivWithFunc
    private List<FunctionRef> functionRefs;
    private List<FunctionArgsDef> functionArgsDefs;

    private final NodePosition pos;

    public GrantRevokePrivilegeObjects() {
        this(NodePosition.ZERO);
    }

    public GrantRevokePrivilegeObjects(NodePosition pos) {
        this.pos = pos;
    }

    public List<List<String>> getPrivilegeObjectNameTokensList() {
        return privilegeObjectNameTokensList;
    }

    public void setPrivilegeObjectNameTokensList(List<List<String>> privilegeObjectNameTokensList) {
        this.privilegeObjectNameTokensList = privilegeObjectNameTokensList;
    }

    public List<UserRef> getUserPrivilegeObjectList() {
        return userPrivilegeObjectList;
    }

    public void setUserPrivilegeObjectList(List<UserRef> userPrivilegeObjectList) {
        this.userPrivilegeObjectList = userPrivilegeObjectList;
    }

    public List<FunctionRef> getFunctionRefs() {
        return functionRefs;
    }

    public void setFunctionRefs(List<FunctionRef> functionRefs) {
        this.functionRefs = functionRefs;
    }

    public List<FunctionArgsDef> getFunctionArgsDefs() {
        return functionArgsDefs;
    }

    public void setFunctionArgsDefs(List<FunctionArgsDef> functionArgsDefs) {
        this.functionArgsDefs = functionArgsDefs;
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }
}
