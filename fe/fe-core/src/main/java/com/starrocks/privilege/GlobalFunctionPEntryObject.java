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

package com.starrocks.privilege;

import com.google.common.base.Objects;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Function;
import com.starrocks.server.GlobalStateMgr;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class GlobalFunctionPEntryObject implements PEntryObject {
    @SerializedName(value = "f")
    protected String functionSig;
    protected static final String ALL_FUNCTIONS_SIG = "AS"; // AS represent all functions

    public static final String FUNC_NOT_FOUND = "funcNotFound";

    public static GlobalFunctionPEntryObject generate(GlobalStateMgr mgr, List<String> tokens)
            throws PrivilegeException {
        if (tokens.size() != 1) {
            throw new PrivilegeException("invalid object tokens: " + tokens);
        }
        if (tokens.get(0).equals(FUNC_NOT_FOUND)) {
            throw new PrivilegeException("func not found");
        }
        String funcSig = tokens.get(0);
        return new GlobalFunctionPEntryObject(funcSig);
    }

    public static GlobalFunctionPEntryObject generate(
            GlobalStateMgr mgr, List<String> allTypes, String restrictType, String restrictName)
            throws PrivilegeException {
        if (allTypes.size() == 1) {
            if (!StringUtils.isEmpty(restrictType)) {
                throw new PrivilegeException("ALL GLOBAL FUNCTIONS must be restricted withour any database");
            }
            return new GlobalFunctionPEntryObject(ALL_FUNCTIONS_SIG);
        } else {
            throw new PrivilegeException("invalid ALL statement for functions!");
        }
    }

    protected GlobalFunctionPEntryObject(String functionSig) {
        this.functionSig = functionSig;
    }

    @Override
    public boolean match(Object obj) {
        if (!(obj instanceof GlobalFunctionPEntryObject)) {
            return false;
        }
        GlobalFunctionPEntryObject other = (GlobalFunctionPEntryObject) obj;
        if (other.functionSig.equals(ALL_FUNCTIONS_SIG)) {
            return true;
        }
        return other.functionSig.equals(this.functionSig);
    }

    @Override
    public boolean isFuzzyMatching() {
        return false;
    }

    @Override
    public boolean validate(GlobalStateMgr globalStateMgr) {
        Function targetFunc = null;
        for (Function f : globalStateMgr.getGlobalFunctionMgr().getFunctions()) {
            if (f.signatureString().equals(this.functionSig)) {
                targetFunc = f;
            }
        }
        return targetFunc != null;
    }

    @Override
    public int compareTo(PEntryObject o) {
        return 0;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(functionSig);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        GlobalFunctionPEntryObject that = (GlobalFunctionPEntryObject) obj;
        return Objects.equal(functionSig, that.functionSig);
    }

    @Override
    public PEntryObject clone() {
        return new GlobalFunctionPEntryObject(functionSig);
    }
}