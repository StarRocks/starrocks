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
import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Function;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;

public class GlobalFunctionPEntryObject implements PEntryObject {
    public static final String ALL_GLOBAL_FUNCTION_SIGS = "AGFS"; // AS represent all global functions
    public static final String FUNC_NOT_FOUND = "funcNotFound";

    @SerializedName(value = "f")
    protected String functionSig;

    public String getFunctionSig() {
        return functionSig;
    }

    public static GlobalFunctionPEntryObject generate(GlobalStateMgr mgr, List<String> tokens)
            throws PrivilegeException {
        if (tokens.size() != 1) {
            throw new PrivilegeException("invalid object tokens: " + tokens);
        }
        if (tokens.get(0).equals(FUNC_NOT_FOUND)) {
            throw new PrivObjNotFoundException("func not found");
        }
        String funcSig = tokens.get(0);
        return new GlobalFunctionPEntryObject(funcSig);
    }

    public static GlobalFunctionPEntryObject generate(
            GlobalStateMgr mgr, List<String> allTypes, String restrictType, String restrictName)
            throws PrivilegeException {
        if (allTypes.size() == 1) {
            Preconditions.checkState(allTypes.get(0).equals(ObjectType.GLOBAL_FUNCTION.getPlural()));
            Preconditions.checkArgument(restrictType == null);
            Preconditions.checkArgument(restrictName == null);
            return new GlobalFunctionPEntryObject(ALL_GLOBAL_FUNCTION_SIGS);
        } else {
            throw new PrivilegeException("Invalid ALL statement for global functions!");
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
        if (other.functionSig.equals(ALL_GLOBAL_FUNCTION_SIGS)) {
            return true;
        }
        return other.functionSig.equals(this.functionSig);
    }

    @Override
    public boolean isFuzzyMatching() {
        return functionSig.equals(ALL_GLOBAL_FUNCTION_SIGS);
    }

    @Override
    public boolean validate(GlobalStateMgr globalStateMgr) {
        Function targetFunc = null;
        for (Function f : globalStateMgr.getGlobalFunctionMgr().getFunctions()) {
            if (f.signatureString().equals(this.functionSig)) {
                targetFunc = f;
                break;
            }
        }
        return targetFunc != null;
    }

    @Override
    public int compareTo(PEntryObject obj) {
        if (!(obj instanceof GlobalFunctionPEntryObject)) {
            throw new ClassCastException("cannot cast " + obj.getClass().toString() + " to " + this.getClass());
        }

        GlobalFunctionPEntryObject o = (GlobalFunctionPEntryObject) obj;
        if (functionSig.equals(ALL_GLOBAL_FUNCTION_SIGS)) {
            return -1;
        } else if (o.functionSig.equals(ALL_GLOBAL_FUNCTION_SIGS)) {
            return 1;
        } else {
            return functionSig.compareTo(o.functionSig);
        }
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