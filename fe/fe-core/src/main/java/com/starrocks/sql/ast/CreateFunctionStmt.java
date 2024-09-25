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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.TypeDef;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.sql.parser.NodePosition;

import java.util.HashMap;
import java.util.Map;

// create a user define function
public class CreateFunctionStmt extends DdlStmt {
    public static final String FILE_KEY = "file";
    public static final String SYMBOL_KEY = "symbol";
    public static final String MD5_CHECKSUM = "md5";
    public static final String TYPE_KEY = "type";
    public static final String ISOLATION_KEY = "isolation";
    public static final String TYPE_STARROCKS_JAR = "StarrocksJar";
    public static final String TYPE_STARROCKS_PYTHON = "Python";
    public static final String EVAL_METHOD_NAME = "evaluate";
    public static final String CREATE_METHOD_NAME = "create";
    public static final String DESTROY_METHOD_NAME = "destroy";
    public static final String SERIALIZE_METHOD_NAME = "serialize";
    public static final String UPDATE_METHOD_NAME = "update";
    public static final String MERGE_METHOD_NAME = "merge";
    public static final String FINALIZE_METHOD_NAME = "finalize";
    public static final String STATE_CLASS_NAME = "State";
    public static final String SERIALIZE_LENGTH_METHOD_NAME = "serializeLength";
    public static final String RETURN_FIELD_NAME = "Return";
    public static final String WINDOW_UPDATE_METHOD_NAME = "windowUpdate";
    public static final String IS_ANALYTIC_NAME = "analytic";
    public static final String PROCESS_METHOD_NAME = "process";
    public static final String INPUT_TYPE = "input";

    private final FunctionName functionName;
    private final boolean isAggregate;
    private final boolean isTable;
    private final FunctionArgsDef argsDef;
    private final TypeDef returnType;
    private final Map<String, String> properties;
    private final String content;
    private final boolean shouldReplaceIfExists;
    private final boolean createIfNotExists;

    // needed item set after analyzed
    private Function function;

    private static final ImmutableMap<PrimitiveType, Class<?>> PRIMITIVE_TYPE_TO_JAVA_CLASS_TYPE =
            new ImmutableMap.Builder<PrimitiveType, Class<?>>()
                    .put(PrimitiveType.BOOLEAN, Boolean.class)
                    .put(PrimitiveType.TINYINT, Byte.class)
                    .put(PrimitiveType.SMALLINT, Short.class)
                    .put(PrimitiveType.INT, Integer.class)
                    .put(PrimitiveType.FLOAT, Float.class)
                    .put(PrimitiveType.DOUBLE, Double.class)
                    .put(PrimitiveType.BIGINT, Long.class)
                    .put(PrimitiveType.CHAR, String.class)
                    .put(PrimitiveType.VARCHAR, String.class)
                    .build();

    public CreateFunctionStmt(String functionType,
                              FunctionName functionName,
                              FunctionArgsDef argsDef,
                              TypeDef returnType,
                              Map<String, String> properties,
                              String content,
                              boolean shouldReplaceIfExists,
                              boolean createIfNotExists) {
        this(functionType,
                functionName,
                argsDef,
                returnType,
                properties,
                content,
                shouldReplaceIfExists,
                createIfNotExists,
                NodePosition.ZERO
        );
    }

    public CreateFunctionStmt(String functionType, FunctionName functionName, FunctionArgsDef argsDef,
                              TypeDef returnType, Map<String, String> properties, String content,
                              boolean shouldReplaceIfExists, boolean createIfNotExists, NodePosition pos) {
        super(pos);
        this.functionName = functionName;
        this.isAggregate = functionType.equalsIgnoreCase("AGGREGATE");
        this.isTable = functionType.equalsIgnoreCase("TABLE");
        this.argsDef = argsDef;
        this.returnType = returnType;
        this.content = content;
        this.shouldReplaceIfExists = shouldReplaceIfExists;
        this.createIfNotExists = createIfNotExists;
        if (properties == null) {
            this.properties = ImmutableSortedMap.of();
        } else {
            Map<String, String> lowerCaseProperties = new HashMap<>();
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                lowerCaseProperties.put(entry.getKey().toLowerCase(), entry.getValue());
            }

            this.properties = ImmutableSortedMap.copyOf(lowerCaseProperties, String.CASE_INSENSITIVE_ORDER);
        }
    }

    public FunctionName getFunctionName() {
        return functionName;
    }

    public Function getFunction() {
        return function;
    }

    public boolean isScalar() {
        return !isTable() && !isAggregate();
    }

    public boolean isTable() {
        return isTable;
    }

    public boolean isAggregate() {
        return isAggregate;
    }

    public FunctionArgsDef getArgsDef() {
        return argsDef;
    }

    public TypeDef getReturnType() {
        return returnType;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getLangType() {
        return properties.get(TYPE_KEY);
    }

    public String getContent() {
        return content;
    }

    public void setFunction(Function function) {
        this.function = function;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }

    public boolean shouldReplaceIfExists() {
        return shouldReplaceIfExists;
    }

    public boolean createIfNotExists() {
        return createIfNotExists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateFunctionStatement(this, context);
    }
}
