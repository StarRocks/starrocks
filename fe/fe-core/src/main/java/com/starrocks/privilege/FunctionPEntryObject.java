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
import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class FunctionPEntryObject implements PEntryObject {

    @SerializedName(value = "d")
    protected long databaseId;
    @SerializedName(value = "f")
    protected String functionSig;
    protected static final long ALL_DATABASE_ID = -2; // -2 represent all databases
    protected static final String ALL_FUNCTIONS_SIG = "AS"; // AS represent all functions

    public static final String FUNC_NOT_FOUND = "funcNotFound";

    public static FunctionPEntryObject generate(GlobalStateMgr mgr, List<String> tokens) throws PrivilegeException {
        if (tokens.size() != 2) {
            throw new PrivilegeException("invalid object tokens, should have two: " + tokens);
        }
        if (tokens.get(1).equals(FUNC_NOT_FOUND)) {
            throw new PrivObjNotFoundException("func not found");
        }
        Database database = mgr.getDb(tokens.get(0));
        if (database == null) {
            throw new PrivObjNotFoundException("cannot find db: " + tokens.get(0));
        }
        String funcSig = tokens.get(1);
        return new FunctionPEntryObject(database.getId(), funcSig);
    }

    public static FunctionPEntryObject generate(
            GlobalStateMgr mgr, List<String> allTypes, String restrictType, String restrictName)
            throws PrivilegeException {
        if (allTypes.size() == 1) {
            if (StringUtils.isEmpty(restrictType)
                    || !restrictType.equals(ObjectType.DATABASE.toString())
                    || StringUtils.isEmpty(restrictName)) {
                throw new PrivilegeException("ALL FUNCTIONS must be restricted with database!");
            }

            Database database = mgr.getDb(restrictName);
            if (database == null) {
                throw new PrivilegeException("cannot find db: " + restrictName);
            }
            return new FunctionPEntryObject(database.getId(), ALL_FUNCTIONS_SIG);
        } else if (allTypes.size() == 2) {
            if (!allTypes.get(1).equals(ObjectType.DATABASE.getPlural())) {
                throw new PrivilegeException(
                        "ALL FUNCTIONS must be restricted with ALL DATABASES instead of ALL " + allTypes.get(1));
            }
            return new FunctionPEntryObject(ALL_DATABASE_ID, ALL_FUNCTIONS_SIG);
        } else {
            throw new PrivilegeException("invalid ALL statement for functions!");
        }
    }

    protected FunctionPEntryObject(long databaseId, String functionSig) {
        this.databaseId = databaseId;
        this.functionSig = functionSig;
    }

    @Override
    public boolean match(Object obj) {
        if (!(obj instanceof FunctionPEntryObject)) {
            return false;
        }
        FunctionPEntryObject other = (FunctionPEntryObject) obj;
        if (other.databaseId == ALL_DATABASE_ID) {
            return true;
        }
        if (other.functionSig.equals(ALL_FUNCTIONS_SIG)) {
            return databaseId == other.databaseId;
        }
        return other.databaseId == this.databaseId &&
                other.functionSig.equals(this.functionSig);
    }

    @Override
    public boolean isFuzzyMatching() {
        return databaseId == ALL_DATABASE_ID || functionSig.equals(ALL_FUNCTIONS_SIG);
    }

    @Override
    public boolean validate(GlobalStateMgr globalStateMgr) {
        Database db = globalStateMgr.getDbIncludeRecycleBin(this.databaseId);
        if (db == null) {
            return false;
        }
        Function targetFunc = null;
        for (Function f : db.getFunctions()) {
            if (f.signatureString().equals(this.functionSig)) {
                targetFunc = f;
                break;
            }
        }
        return targetFunc != null;
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
            if (functionSig.equals(ALL_FUNCTIONS_SIG)) {
                return -1;
            } else if (o.functionSig.equals(ALL_FUNCTIONS_SIG)) {
                return 1;
            } else {
                return functionSig.compareTo(o.functionSig);
            }
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(functionSig, databaseId);
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
        return Objects.equal(functionSig, that.functionSig) &&
                Objects.equal(databaseId, that.databaseId);
    }

    @Override
    public PEntryObject clone() {
        return new FunctionPEntryObject(databaseId, functionSig);
    }
}