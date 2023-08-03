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

package com.starrocks.analysis;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.thrift.TFunctionName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Class to represent a function name. Function names are specified as
 * db.function_name.
 */
public class FunctionName implements Writable {
    public static final String GLOBAL_UDF_DB = "__global_udf_db__";

    @SerializedName(value = "db")
    private String db_;
    @SerializedName(value = "fn")
    private String fn_;

    private FunctionName() {
    }

    public FunctionName(String db, String fn) {
        db_ = db;
        fn_ = fn.toLowerCase();
    }

    public FunctionName(String fn) {
        db_ = null;
        fn_ = fn.toLowerCase();
    }

    public FunctionName(TFunctionName thriftName) {
        db_ = thriftName.db_name;
        fn_ = thriftName.function_name.toLowerCase();
    }

    // Same as FunctionName but for builtins and we'll leave the case
    // as is since we aren't matching by string.
    public static FunctionName createBuiltinName(String fn) {
        FunctionName name = new FunctionName(fn);
        name.fn_ = fn;
        return name;
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
        return Objects.equals(db_, o.db_) && Objects.equals(fn_, o.fn_);
    }

    public String getDb() {
        return db_;
    }

    public void setDb(String db) {
        db_ = db;
    }

    public String getFunction() {
        return fn_;
    }

    public boolean isGlobalFunction() {
        return GLOBAL_UDF_DB.equals(db_);
    }

    public void setAsGlobalFunction() {
        db_ = GLOBAL_UDF_DB;
    }

    @Override
    public String toString() {
        if (db_ == null) {
            return fn_;
        }
        return db_ + "." + fn_;
    }

    public void analyze(String defaultDb) throws AnalysisException {
        if (fn_.length() == 0) {
            throw new AnalysisException("Function name can not be empty.");
        }
        for (int i = 0; i < fn_.length(); ++i) {
            if (!isValidCharacter(fn_.charAt(i))) {
                throw new AnalysisException(
                        "Function names must be all alphanumeric or underscore. " +
                                "Invalid name: " + fn_);
            }
        }
        if (Character.isDigit(fn_.charAt(0))) {
            throw new AnalysisException("Function cannot start with a digit: " + fn_);
        }
        if (db_ == null) {
            db_ = defaultDb;
            if (Strings.isNullOrEmpty(db_)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }
    }

    private boolean isValidCharacter(char c) {
        return Character.isLetterOrDigit(c) || c == '_';
    }

    public TFunctionName toThrift() {
        TFunctionName name = new TFunctionName(fn_);
        name.setDb_name(db_);
        name.setFunction_name(fn_);
        return name;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (db_ != null) {
            out.writeBoolean(true);
            // compatible with old version
            Text.writeString(out, ClusterNamespace.getFullName(db_));
        } else {
            out.writeBoolean(false);
        }
        Text.writeString(out, fn_);
    }

    public void readFields(DataInput in) throws IOException {
        if (in.readBoolean()) {
            // compatible with old version
            db_ = ClusterNamespace.getNameFromFullName(Text.readString(in));
        }
        fn_ = Text.readString(in);
    }

    public static FunctionName read(DataInput in) throws IOException {
        FunctionName functionName = new FunctionName();
        functionName.readFields(in);
        return functionName;
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hashCode(db_) + Objects.hashCode(fn_);
    }
}
