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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/Function.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.common.Pair;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.sql.ast.HdfsURI;
import com.starrocks.thrift.TFunction;
import com.starrocks.thrift.TFunctionBinaryType;
import org.apache.commons.lang.ArrayUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.starrocks.common.io.IOUtils.readOptionStringOrNull;
import static com.starrocks.common.io.IOUtils.writeOptionString;

/**
 * Base class for all functions.
 */
public class Function implements Writable {
    // Enum for how to compare function signatures.
    // For decimal types, the type in the function can be a wildcard, i.e. decimal(*,*).
    // The wildcard can *only* exist as function type, the caller will always be a
    // fully specified decimal.
    // For the purposes of function type resolution, decimal(*,*) will match exactly
    // with any fully specified decimal (i.e. fn(decimal(*,*)) matches identically for
    // the call to fn(decimal(1,0)).
    public enum CompareMode {
        // Two signatures are identical if the number of arguments and their types match
        // exactly and either both signatures are varargs or neither.
        IS_IDENTICAL,

        // Two signatures are indistinguishable if there is no way to tell them apart
        // when matching a particular instantiation. That is, their fixed arguments
        // match exactly and the remaining varargs have the same type.
        // e.g. fn(int, int, int) and fn(int...)
        // Argument types that are NULL are ignored when doing this comparison.
        // e.g. fn(NULL, int) is indistinguishable from fn(int, int)
        IS_INDISTINGUISHABLE,

        // X is a supertype of Y if Y.arg[i] can be strictly implicitly cast to X.arg[i]. If
        /// X has vargs, the remaining arguments of Y must be strictly implicitly castable
        // to the var arg type. The key property this provides is that X can be used in place
        // of Y. e.g. fn(int, double, string...) is a supertype of fn(tinyint, float, string,
        // string)
        IS_SUPERTYPE_OF,

        // Nonstrict supertypes broaden the definition of supertype to accept implicit casts
        // of arguments that may result in loss of precision - e.g. decimal to float.
        IS_NONSTRICT_SUPERTYPE_OF,
    }

    // for vectorized engine, function-id
    @SerializedName(value = "fid")
    protected long functionId;

    @SerializedName(value = "name")
    private FunctionName name;

    @SerializedName(value = "retType")
    private Type retType;

    // Array of parameter types.  empty array if this function does not have parameters.
    @SerializedName(value = "argTypes")
    private Type[] argTypes;

    @SerializedName(value = "argNames")
    private String[] argNames;

    // If true, this function has variable arguments.
    // TODO: we don't currently support varargs with no fixed types. i.e. fn(...)
    @SerializedName(value = "hasVarArgs")
    private boolean hasVarArgs;

    // If true (default), this function is called directly by the user. For operators,
    // this is false. If false, it also means the function is not visible from
    // 'show functions'.
    @SerializedName(value = "userVisible")
    private boolean userVisible;

    @SerializedName(value = "binaryType")
    private TFunctionBinaryType binaryType;

    // Absolute path in HDFS for the binary that contains this function.
    // e.g. /udfs/udfs.jar
    @SerializedName(value = "location")
    private HdfsURI location;

    // library's checksum to make sure all backends use one library to serve user's request
    @SerializedName(value = "checksum")
    protected String checksum = "";

    // Function id, every function has a unique id. Now all built-in functions' id is 0
    private long id = 0;
    // User specified function name e.g. "Add"

    private boolean isPolymorphic = false;

    // If low cardinality string column with global dict, for some string functions,
    // we could evaluate the function only with the dict content, not all string column data.
    private boolean couldApplyDictOptimize = false;

    private boolean isNullable = true;

    private Vector<Pair<String, Expr>> defaultArgExprs;
    // Only used for serialization
    protected Function() {
    }

    public Function(FunctionName name, Type[] argTypes, Type retType, boolean varArgs) {
        this(0, name, argTypes, retType, varArgs);
    }

    public Function(FunctionName name, Type[] argTypes, String[] argNames, Type retType, boolean varArgs) {
        this(0, name, argTypes, argNames, retType, varArgs);
    }

    public Function(FunctionName name, List<Type> args, Type retType, boolean varArgs) {
        this(0, name, args, retType, varArgs);
    }

    public Function(long id, FunctionName name, Type[] argTypes, Type retType, boolean hasVarArgs) {
        this.id = id;
        this.functionId = id;
        this.name = name;
        this.hasVarArgs = hasVarArgs;
        if (argTypes == null) {
            this.argTypes = new Type[0];
        } else {
            this.argTypes = argTypes;
        }
        this.retType = retType;
        this.isPolymorphic = Arrays.stream(this.argTypes).anyMatch(Type::isPseudoType);
    }

    public Function(long id, FunctionName name, Type[] argTypes, String[] argNames, Type retType, boolean hasVarArgs) {
        this.id = id;
        this.functionId = id;
        this.name = name;
        this.hasVarArgs = hasVarArgs;
        if (argTypes == null) {
            this.argTypes = new Type[0];
        } else {
            this.argTypes = argTypes;
        }
        this.argNames = argNames;
        this.retType = retType;
        this.isPolymorphic = Arrays.stream(this.argTypes).anyMatch(Type::isPseudoType);
    }

    public Function(long id, FunctionName name, List<Type> argTypes, Type retType, boolean hasVarArgs) {
        this.id = id;
        this.functionId = id;
        this.name = name;
        this.hasVarArgs = hasVarArgs;
        if (argTypes == null) {
            this.argTypes = new Type[0];
        } else {
            this.argTypes = argTypes.toArray(new Type[0]);
        }
        this.retType = retType;
        this.isPolymorphic = Arrays.stream(this.argTypes).anyMatch(Type::isPseudoType);
    }

    // copy constructor
    public Function(Function other) {
        id = other.id;
        name = other.name;
        retType = other.retType;
        argTypes = other.argTypes;
        argNames = other.argNames;
        hasVarArgs = other.hasVarArgs;
        userVisible = other.userVisible;
        location = other.location;
        binaryType = other.binaryType;
        checksum = other.checksum;
        functionId = other.functionId;
        isPolymorphic = other.isPolymorphic;
        couldApplyDictOptimize = other.couldApplyDictOptimize;
        isNullable = other.isNullable;
    }

    public FunctionName getFunctionName() {
        return name;
    }

    public String functionName() {
        return name.getFunction();
    }

    public String dbName() {
        return name.getDb();
    }

    public Type getReturnType() {
        return retType;
    }

    public Type[] getArgs() {
        return argTypes;
    }

    public String[] getArgNames() {
        return argNames;
    }

    public boolean hasNamedArg() {
        return argNames != null && argNames.length > 0;
    }

    // Returns the number of arguments to this function.
    public int getNumArgs() {
        return argTypes.length;
    }

    public HdfsURI getLocation() {
        return location;
    }

    public void setLocation(HdfsURI loc) {
        location = loc;
    }

    public TFunctionBinaryType getBinaryType() {
        return binaryType;
    }

    public void setBinaryType(TFunctionBinaryType type) {
        binaryType = type;
    }

    public void setArgNames(List<String> names) {
        if (names != null) {
            argNames = names.toArray(new String[0]);
            Preconditions.checkState(argNames.length == argTypes.length);
        }
    }

    public void setDefaultNamedArgs(Vector<Pair<String, Expr>> defaultArgExprs) {
        this.defaultArgExprs = defaultArgExprs;
    }

    public List<Expr> getLastDefaultsFromN(int n) {
        if (defaultArgExprs == null || n >= argTypes.length || n < getRequiredArgNum()) {
            return null;
        }
        return defaultArgExprs.subList(n - getRequiredArgNum(), defaultArgExprs.size()).
                stream().map(x -> x.second).collect(Collectors.toList());
    }

    public Expr getDefaultNamedExpr(String argName) {
        if (defaultArgExprs == null) {
            return null;
        }
        for (Pair<String, Expr> arg : defaultArgExprs) {
            if (arg.first.equals(argName)) {
                return arg.second;
            }
        }
        return null;
    }

    public int getRequiredArgNum() {
        return argTypes.length - (defaultArgExprs == null ? 0 : defaultArgExprs.size());
    }

    public boolean hasVarArgs() {
        return hasVarArgs;
    }

    public boolean isUserVisible() {
        return userVisible;
    }

    public void setUserVisible(boolean userVisible) {
        this.userVisible = userVisible;
    }

    // TODO: It's dangerous to change this directly because it may change the global function state.
    // Make sure you use a copy of the global function.
    public void setArgsType(Type[] newTypes) {
        argTypes = newTypes;
    }

    public Type getVarArgsType() {
        if (!hasVarArgs) {
            return Type.INVALID;
        }
        Preconditions.checkState(argTypes.length > 0);
        return argTypes[argTypes.length - 1];
    }

    public boolean isPolymorphic() {
        return isPolymorphic;
    }

    public long getFunctionId() {
        return functionId;
    }

    public void setFunctionId(long functionId) {
        this.functionId = functionId;
    }

    public void setHasVarArgs(boolean v) {
        hasVarArgs = v;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public void setChecksum(String checksum) {
        this.checksum = checksum;
    }

    public String getChecksum() {
        return checksum;
    }

    // TODO: It's dangerous to change this directly because it may change the global function state.
    // Make sure you use a copy of the global function.
    public void setRetType(Type retType) {
        this.retType = retType;
    }

    // TODO(cmy): Currently we judge whether it is UDF by wheter the 'location' is set.
    // Maybe we should use a separate variable to identify,
    // but additional variables need to modify the persistence information.
    public boolean isUdf() {
        return location != null;
    }

    // Returns a string with the signature in human readable format:
    // FnName(argtype1, argtyp2).  e.g. Add(int, int)
    public String signatureString() {
        StringBuilder sb = new StringBuilder();
        sb.append(name.getFunction()).append("(").append(Joiner.on(", ").join(argTypes));
        if (hasVarArgs) {
            sb.append("...");
        }
        sb.append(")");
        return sb.toString();
    }

    public boolean isNullable() {
        return isNullable;
    }

    public void setIsNullable(boolean isNullable) {
        this.isNullable = isNullable;
    }

    public boolean isCouldApplyDictOptimize() {
        return couldApplyDictOptimize;
    }

    public void setCouldApplyDictOptimize(boolean couldApplyDictOptimize) {
        this.couldApplyDictOptimize = couldApplyDictOptimize;
    }

    // Compares this to 'other' for mode.
    public boolean compare(Function other, CompareMode mode) {
        switch (mode) {
            case IS_IDENTICAL:
                return isIdentical(other);
            case IS_INDISTINGUISHABLE:
                return isIndistinguishable(other);
            case IS_SUPERTYPE_OF:
                return isSubtype(other);
            case IS_NONSTRICT_SUPERTYPE_OF:
                return isAssignCompatible(other);
            default:
                Preconditions.checkState(false);
                return false;
        }
    }

    private boolean compareNamedArguments(Function other, int start, BiFunction<Type, Type, Boolean> notMatch) {
        if (!this.hasNamedArg() || other.argNames.length > this.argNames.length ||
                other.hasVarArgs || this.hasVarArgs) {
            return false;
        }
        boolean[] mask = new boolean[other.argTypes.length];
        for (int i = start; i < this.argTypes.length; ++i) {
            boolean found = false;
            for (int j = start; j < other.argTypes.length && !found; ++j) {
                if (!mask[j] && this.argNames[i].equals(other.argNames[j])) {
                    if (notMatch.apply(other.argTypes[j], argTypes[i])) {
                        return false;
                    }
                    found = true;
                    mask[j] = true;
                }
            }
            if (!found) {
                // not default
                if (this.getDefaultNamedExpr(this.argNames[i]) == null) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean comparePositionalArguments(Function other, int start, BiFunction<Type, Type, Boolean> notMatch) {
        if (other.argTypes.length > this.argTypes.length ||
                other.hasVarArgs || this.hasVarArgs || other.argTypes.length < getRequiredArgNum()) {
            return false;
        }
        for (int i = start; i < other.argTypes.length; ++i) {
            if (notMatch.apply(other.argTypes[i], argTypes[i])) {
                return false;
            }
        }
        return true;
    }
    /**
     * Returns true if 'this' is a supertype of 'other'. Each argument in other must
     * be implicitly castable to the matching argument in this.
     * TODO: look into how we resolve implicitly castable functions. Is there a rule
     * for "most" compatible or maybe return an error if it is ambiguous?
     */
    private boolean isSubtype(Function other) {
        int startArgIndex = 0;
        String functionName = other.getFunctionName().getFunction();
        // If function first arg must be boolean, we don't check it.
        if (functionName.equalsIgnoreCase("if")) {
            startArgIndex = 1;
        }
        if (other.hasNamedArg()) {
            return compareNamedArguments(other, startArgIndex,
                    (Type ot, Type m) -> !ot.matchesType(m) && !Type.isImplicitlyCastable(ot, m, true));
        } else if (this.defaultArgExprs != null && !other.hasVarArgs) {
            // positional args with defaults in table functions
            return comparePositionalArguments(other, startArgIndex,
                    (Type ot, Type m) -> !ot.matchesType(m) && !Type.isImplicitlyCastable(ot, m, true));
        } else {
            if (!this.hasVarArgs && other.argTypes.length != this.argTypes.length) {
                return false;
            }
            if (this.hasVarArgs && other.argTypes.length < this.argTypes.length) {
                return false;
            }
            for (int i = startArgIndex; i < this.argTypes.length; ++i) {
                // Normally, if type A matches type B, then A and B must be implicitly castable,
                // but if one of the types is pseudotype, this rule does not hold anymore, so here
                // we check type match first.
                if (other.argTypes[i].matchesType(this.argTypes[i])) {
                    continue;
                }
                if (!Type.isImplicitlyCastable(other.argTypes[i], this.argTypes[i], true)) {
                    return false;
                }
            }

            // Check trailing varargs.
            if (this.hasVarArgs) {
                for (int i = this.argTypes.length; i < other.argTypes.length; ++i) {
                    if (other.argTypes[i].matchesType(getVarArgsType())) {
                        continue;
                    }
                    if (!Type.isImplicitlyCastable(other.argTypes[i], getVarArgsType(), true)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    // return true if 'this' is assign-compatible from 'other'.
    // Each argument in 'other' must be assign-compatible to the matching argument in 'this'.
    private boolean isAssignCompatible(Function other) {
        if (other.hasNamedArg()) {
            return compareNamedArguments(other, 0,
                    (Type ot, Type m) -> !ot.matchesType(m) && !Type.canCastTo(ot, m));
        } else if (this.defaultArgExprs != null && !other.hasVarArgs) {
            // positional args with defaults in table functions
            return comparePositionalArguments(other, 0,
                    (Type ot, Type m) -> !ot.matchesType(m) && !Type.canCastTo(ot, m));
        } else {
            if (!this.hasVarArgs && other.argTypes.length != this.argTypes.length) {
                return false;
            }
            if (this.hasVarArgs && other.argTypes.length < this.argTypes.length) {
                return false;
            }
            for (int i = 0; i < this.argTypes.length; ++i) {
                if (other.argTypes[i].matchesType(this.argTypes[i])) {
                    continue;
                }
                if (!Type.canCastTo(other.argTypes[i], argTypes[i])) {
                    return false;
                }
            }
            // Check trailing varargs.
            if (this.hasVarArgs) {
                for (int i = this.argTypes.length; i < other.argTypes.length; ++i) {
                    if (other.argTypes[i].matchesType(getVarArgsType())) {
                        continue;
                    }
                    if (!Type.canCastTo(other.argTypes[i], getVarArgsType())) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private boolean isIdentical(Function o) {
        if (!o.name.equals(name)) {
            return false;
        }
        if (o.argTypes.length != this.argTypes.length) {
            return false;
        }
        if (o.hasVarArgs != this.hasVarArgs) {
            return false;
        }

        if (o.hasNamedArg()) {
            return compareNamedArguments(o, 0,
                    (Type ot, Type m) -> !ot.matchesType(m));
        } else if (this.defaultArgExprs != null && !o.hasVarArgs) { // positional args with defaults in table functions
            return comparePositionalArguments(o, 0,
                    (Type ot, Type m) -> !ot.matchesType(m));
        } else {
            for (int i = 0; i < this.argTypes.length; ++i) {
                if (!o.argTypes[i].matchesType(this.argTypes[i])) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean isIndistinguishable(Function o) {
        if (!o.name.equals(name)) {
            return false;
        }
        int minArgs = Math.min(o.argTypes.length, this.argTypes.length);
        // The first fully specified args must be identical.
        if (o.hasNamedArg()) {
            return compareNamedArguments(o, 0,
                    (Type ot, Type m) -> !ot.isNull() && !m.isNull() && !ot.matchesType(m));
        } else if (this.defaultArgExprs != null && !o.hasVarArgs) { // positional args with defaults in table functions
            return comparePositionalArguments(o, 0,
                    (Type ot, Type m) -> !ot.isNull() && !m.isNull() && !ot.matchesType(m));
        } else {
            for (int i = 0; i < minArgs; ++i) {
                if (o.argTypes[i].isNull() || this.argTypes[i].isNull()) {
                    continue;
                }
                if (!o.argTypes[i].matchesType(this.argTypes[i])) {
                    return false;
                }
            }
            if (o.argTypes.length == this.argTypes.length) {
                return true;
            }

            if (o.hasVarArgs && this.hasVarArgs) {
                if (!o.getVarArgsType().matchesType(this.getVarArgsType())) {
                    return false;
                }
                if (this.getNumArgs() > o.getNumArgs()) {
                    for (int i = minArgs; i < this.getNumArgs(); ++i) {
                        if (this.argTypes[i].isNull()) {
                            continue;
                        }
                        if (!this.argTypes[i].matchesType(o.getVarArgsType())) {
                            return false;
                        }
                    }
                } else {
                    for (int i = minArgs; i < o.getNumArgs(); ++i) {
                        if (o.argTypes[i].isNull()) {
                            continue;
                        }
                        if (!o.argTypes[i].matchesType(this.getVarArgsType())) {
                            return false;
                        }
                    }
                }
                return true;
            } else if (o.hasVarArgs) {
                // o has var args so check the remaining arguments from this
                if (o.getNumArgs() > minArgs) {
                    return false;
                }
                for (int i = minArgs; i < this.getNumArgs(); ++i) {
                    if (this.argTypes[i].isNull()) {
                        continue;
                    }
                    if (!this.argTypes[i].matchesType(o.getVarArgsType())) {
                        return false;
                    }
                }
                return true;
            } else if (this.hasVarArgs) {
                // this has var args so check the remaining arguments from s
                if (this.getNumArgs() > minArgs) {
                    return false;
                }
                for (int i = minArgs; i < o.getNumArgs(); ++i) {
                    if (o.argTypes[i].isNull()) {
                        continue;
                    }
                    if (!o.argTypes[i].matchesType(this.getVarArgsType())) {
                        return false;
                    }
                }
                return true;
            } else {
                // Neither has var args and the lengths don't match
                return false;
            }
        }
    }

    public TFunction toThrift() {
        TFunction fn = new TFunction();
        fn.setName(name.toThrift());
        fn.setBinary_type(binaryType);
        if (location != null) {
            fn.setHdfs_location(location.toString());
        }
        fn.setArg_types(Type.toThrift(argTypes));
        fn.setRet_type(getReturnType().toThrift());
        fn.setHas_var_args(hasVarArgs);
        fn.setId(id);
        fn.setFid(functionId);
        if (!checksum.isEmpty()) {
            fn.setChecksum(checksum);
        }
        fn.setCould_apply_dict_optimize(couldApplyDictOptimize);
        return fn;
    }

    // Child classes must override this function.
    public String toSql(boolean ifNotExists) {
        return "";
    }

    public static Function getFunction(List<Function> fns, Function desc, CompareMode mode) {
        if (fns == null) {
            return null;
        }
        // First check for identical
        for (Function f : fns) {
            if (f.compare(desc, Function.CompareMode.IS_IDENTICAL)) {
                return f;
            }
        }
        if (mode == Function.CompareMode.IS_IDENTICAL) {
            return null;
        }

        // Next check for indistinguishable
        for (Function f : fns) {
            if (f.compare(desc, Function.CompareMode.IS_INDISTINGUISHABLE)) {
                return f;
            }
        }
        if (mode == Function.CompareMode.IS_INDISTINGUISHABLE) {
            return null;
        }

        // Next check for strict supertypes
        for (Function f : fns) {
            if (f.compare(desc, Function.CompareMode.IS_SUPERTYPE_OF)) {
                return f;
            }
        }
        if (mode == Function.CompareMode.IS_SUPERTYPE_OF) {
            return null;
        }
        // Finally check for non-strict supertypes
        for (Function f : fns) {
            if (f.compare(desc, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF)) {
                return f;
            }
        }
        return null;
    }

    enum FunctionType {
        ORIGIN(0),
        SCALAR(1),
        AGGREGATE(2),
        TABLE(3),
        UNSUPPORTED(-1);

        private int code;

        FunctionType(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        public static FunctionType fromCode(int code) {
            switch (code) {
                case 0:
                    return ORIGIN;
                case 1:
                    return SCALAR;
                case 2:
                    return AGGREGATE;
                case 3:
                    return TABLE;
                default:
                    return UNSUPPORTED;
            }
        }

        public void write(DataOutput output) throws IOException {
            output.writeInt(code);
        }

        public static FunctionType read(DataInput input) throws IOException {
            return fromCode(input.readInt());
        }
    }

    protected void writeFields(DataOutput output) throws IOException {
        output.writeLong(functionId);
        name.write(output);
        ColumnType.write(output, retType);
        output.writeInt(argTypes.length);
        for (Type type : argTypes) {
            ColumnType.write(output, type);
        }
        if (hasNamedArg()) {
            output.writeBoolean(true);
            for (String name : argNames) {
                writeOptionString(output, name);
            }
        } else {
            output.writeBoolean(false);
        }
        output.writeBoolean(hasVarArgs);
        output.writeBoolean(userVisible);
        output.writeInt(binaryType.getValue());
        // write library URL
        String libUrl = "";
        if (location != null) {
            libUrl = location.toString();
        }
        writeOptionString(output, libUrl);
        writeOptionString(output, checksum);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        throw new Error("Origin function cannot be serialized");
    }

    public void readFields(DataInput input) throws IOException {
        id = 0;
        functionId = input.readLong();
        name = FunctionName.read(input);
        retType = ColumnType.read(input);
        int numArgs = input.readInt();
        argTypes = new Type[numArgs];
        for (int i = 0; i < numArgs; ++i) {
            argTypes[i] = ColumnType.read(input);
        }
        boolean hasNamedArg = input.readBoolean();
        if (hasNamedArg) {
            argNames = new String[numArgs];
            for (int i = 0; i < numArgs; ++i) {
                argNames[i] = readOptionStringOrNull(input);
                argNames[i] = argNames[i] == null ? "" : argNames[i];
            }
        }
        hasVarArgs = input.readBoolean();
        userVisible = input.readBoolean();
        binaryType = TFunctionBinaryType.findByValue(input.readInt());

        boolean hasLocation = input.readBoolean();
        if (hasLocation) {
            location = new HdfsURI(Text.readString(input));
        }
        boolean hasChecksum = input.readBoolean();
        if (hasChecksum) {
            checksum = Text.readString(input);
        }
    }

    public static Function read(DataInput input) throws IOException {
        Function function;
        FunctionType functionType = FunctionType.read(input);
        switch (functionType) {
            case SCALAR:
                function = new ScalarFunction();
                break;
            case AGGREGATE:
                function = new AggregateFunction();
                break;
            case TABLE:
                function = new TableFunction();
                break;
            default:
                throw new Error("Unsupported function type, type=" + functionType);
        }
        function.readFields(input);
        return function;
    }

    public String getSignature() {
        StringBuilder sb = new StringBuilder();
        sb.append(name.getFunction()).append("(");
        for (int i = 0; i < argTypes.length; ++i) {
            if (i != 0) {
                sb.append(',');
            }
            sb.append(argTypes[i].getPrimitiveType().toString());
        }
        if (hasVarArgs) {
            sb.append(", ...");
        }
        sb.append(")");
        return sb.toString();
    }

    public String getProperties() {
        return "";
    }

    public List<Comparable> getInfo(boolean isVerbose) {
        List<Comparable> row = Lists.newArrayList();
        if (isVerbose) {
            // signature
            row.add(getSignature());
            // return type
            row.add(getReturnType().getPrimitiveType().toString());
            // function type
            // intermediate type
            if (this instanceof ScalarFunction) {
                row.add("Scalar");
                row.add("NULL");
            } else if (this instanceof AggregateFunction) {
                row.add("Aggregate");
                AggregateFunction aggFunc = (AggregateFunction) this;
                Type intermediateType = aggFunc.getIntermediateType();
                if (intermediateType != null) {
                    row.add(intermediateType.getPrimitiveType().toString());
                } else {
                    row.add("NULL");
                }
            } else {
                TableFunction tableFunc = (TableFunction) this;
                row.add("Table");
                row.add("NULL");
            }
            // property
            row.add(getProperties());
        } else {
            row.add(functionName());
        }
        return row;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, hasVarArgs, argTypes.length);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        return obj != null && obj.getClass() == this.getClass() && isIdentical((Function) obj);
    }


    // just shallow copy
    public Function copy() {
        return new Function(this);
    }

    public Function updateArgType(Type[] newTypes) {
        if (!ArrayUtils.isEquals(argTypes, newTypes)) {
            Function newFunc = copy();
            newFunc.setArgsType(newTypes);
            return newFunc;
        }

        return this;
    }

}
