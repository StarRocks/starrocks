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
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.MetaNotFoundException;

import java.util.List;

public class FunctionPEntryObject implements PEntryObject {

    @SerializedName(value = "d")
    protected long databaseId;

    @SerializedName(value = "functionId")
    protected Long functionId;

    public long getDatabaseId() {
        return databaseId;
    }

    public Long getFunctionId() {
        return functionId;
    }

    protected FunctionPEntryObject(long databaseId, Long functionId) {
        this.databaseId = databaseId;
        this.functionId = functionId;
    }

    public static FunctionPEntryObject generate(GlobalStateMgr mgr, Long databaseId, Long functionId) throws PrivilegeException {
        FunctionPEntryObject functionPEntryObject = new FunctionPEntryObject(databaseId, functionId);
        if (functionId != PrivilegeBuiltinConstants.ALL_FUNCTIONS_ID) {
            if (!functionPEntryObject.validate(mgr)) {
                throw new PrivObjNotFoundException("cannot find function: " + functionId);
            }
        }

        return functionPEntryObject;
    }

    @Override
    public boolean match(Object obj) {
        if (!(obj instanceof FunctionPEntryObject)) {
            return false;
        }
        FunctionPEntryObject other = (FunctionPEntryObject) obj;
        if (other.databaseId == PrivilegeBuiltinConstants.ALL_DATABASE_ID) {
            return true;
        }

        if (other.functionId.equals(PrivilegeBuiltinConstants.ALL_FUNCTIONS_ID)) {
            return databaseId == other.databaseId;
        }
        return other.databaseId == this.databaseId && other.functionId.equals(this.functionId);
    }

    @Override
    public boolean isFuzzyMatching() {
        return databaseId == PrivilegeBuiltinConstants.ALL_DATABASE_ID
                || functionId.equals(PrivilegeBuiltinConstants.ALL_FUNCTIONS_ID);
    }

    @Override
    public boolean validate(GlobalStateMgr globalStateMgr) {
        List<Function> allFunctions;
        if (databaseId == PrivilegeBuiltinConstants.GLOBAL_FUNCTION_DEFAULT_DATABASE_ID) {
            allFunctions = globalStateMgr.getGlobalFunctionMgr().getFunctions();
        } else {
            Database db = globalStateMgr.getLocalMetastore().getDbIncludeRecycleBin(this.databaseId);
            if (db == null) {
                return false;
            }
            allFunctions = db.getFunctions();
        }

        for (Function f : allFunctions) {
            if (f.getFunctionId() == this.functionId) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int compareTo(PEntryObject obj) {
        if (!(obj instanceof FunctionPEntryObject)) {
            throw new ClassCastException("cannot cast " + obj.getClass().toString() + " to " + this.getClass());
        }
        FunctionPEntryObject o = (FunctionPEntryObject) obj;
        if (this.databaseId > o.databaseId) {
            return 1;
        } else if (this.databaseId < o.databaseId) {
            return -1;
        } else {
            if (functionId.equals(PrivilegeBuiltinConstants.ALL_FUNCTIONS_ID)) {
                return -1;
            } else if (o.functionId.equals(PrivilegeBuiltinConstants.ALL_FUNCTIONS_ID)) {
                return 1;
            } else {
                return functionId.compareTo(PrivilegeBuiltinConstants.ALL_FUNCTIONS_ID);
            }
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(databaseId, functionId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        FunctionPEntryObject that = (FunctionPEntryObject) obj;
        return Objects.equal(databaseId, that.databaseId) && Objects.equal(functionId, that.functionId);
    }

    @Override
    public PEntryObject clone() {
        return new FunctionPEntryObject(databaseId, functionId);
    }

    @Override
    public String toString() {
        if (databaseId == PrivilegeBuiltinConstants.GLOBAL_FUNCTION_DEFAULT_DATABASE_ID) {
            if (functionId == PrivilegeBuiltinConstants.ALL_FUNCTIONS_ID) {
                return "ALL GLOBAL FUNCTIONS";
            } else {
                for (Function f : GlobalStateMgr.getCurrentState().getGlobalFunctionMgr().getFunctions()) {
                    if (f.getFunctionId() == functionId) {
                        return f.getSignature();
                    }
                }
                throw new MetaNotFoundException("Can't find database : " + databaseId);
            }
        } else {
            if (databaseId == PrivilegeBuiltinConstants.ALL_DATABASE_ID) {
                return "ALL FUNCTIONS IN ALL DATABASES";
            } else {
                Database database = GlobalStateMgr.getCurrentState().getDb(databaseId);
                if (database == null) {
                    throw new MetaNotFoundException("Can't find database : " + databaseId);
                }

                StringBuilder sb = new StringBuilder();
                if (functionId.equals(PrivilegeBuiltinConstants.ALL_FUNCTIONS_ID)) {
                    sb.append("ALL FUNCTIONS ");
                    sb.append("IN DATABASE ");
                    sb.append(database.getFullName());
                } else {
                    for (Function f : database.getFunctions()) {
                        if (f.getFunctionId() == functionId) {
                            return f.getSignature() + " in DATABASE " + database.getFullName();
                        }
                    }
                    throw new MetaNotFoundException("Can't find database : " + databaseId);
                }
                return sb.toString();
            }
        }
    }
}