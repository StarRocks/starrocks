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

package com.starrocks.sql.ast.expression;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Function;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.io.Writable;
import com.starrocks.thrift.TFunctionName;

import java.util.Objects;

/**
 * Class to represent a function name. Function names are specified as
 * db.function_name.
 */
public class FunctionName implements Writable {
    public static final String GLOBAL_UDF_DB = "__global_udf_db__";

    @SerializedName(value = "db")
    private String db;
    @SerializedName(value = "fn")
    private String fn;

    private FunctionName() {
    }

    public FunctionName(String db, String fn) {
        this.db = db;
        this.fn = fn.toLowerCase();
    }

    public FunctionName(String fn) {
        db = null;
        this.fn = fn.toLowerCase();
    }

    public static FunctionName createFnName(String fn) {
        final String[] dbWithFn = fn.split("\\.");
        if (dbWithFn.length == 2) {
            return new FunctionName(dbWithFn[0], dbWithFn[1]);
        } else {
            return new FunctionName(null, fn);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof FunctionName)) {
            return false;
        }
        FunctionName o = (FunctionName) obj;
        return Objects.equals(db, o.db) && Objects.equals(fn, o.fn);
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getFunction() {
        return fn;
    }

    public boolean isGlobalFunction() {
        return GLOBAL_UDF_DB.equals(db);
    }

    public void setAsGlobalFunction() {
        db = GLOBAL_UDF_DB;
    }

    @Override
    public String toString() {
        if (db == null) {
            return fn;
        }
        return db + "." + fn;
    }

    public void analyze(String defaultDb) {
        if (fn.length() == 0) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Should order by column");
        }
        for (int i = 0; i < fn.length(); ++i) {
            if (!isValidCharacter(fn.charAt(i))) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Function names must be all alphanumeric or underscore. " +
                                "Invalid name: " + fn);
            }
        }
        if (Character.isDigit(fn.charAt(0))) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                    "Function cannot start with a digit: " + fn);
        }
        if (db == null) {
            db = defaultDb;
            if (Strings.isNullOrEmpty(db)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }
    }

    private boolean isValidCharacter(char c) {
        return Character.isLetterOrDigit(c) || c == '_';
    }

    public TFunctionName toThrift() {
        TFunctionName name = new TFunctionName(fn);
        name.setDb_name(db);
        name.setFunction_name(Function.rectifyFunctionName(fn));
        return name;
    }




    @Override
    public int hashCode() {
        return 31 * Objects.hashCode(db) + Objects.hashCode(fn);
    }
}
