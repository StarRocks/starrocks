// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/CreateFunctionStmt.java

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

package com.starrocks.analysis;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.thrift.TFunctionBinaryType;
import org.apache.commons.codec.binary.Hex;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

// create a user define function
public class CreateFunctionStmt extends DdlStmt {
    public static final String OBJECT_FILE_KEY = "object_file";
    public static final String SYMBOL_KEY = "symbol";
    public static final String PREPARE_SYMBOL_KEY = "prepare_fn";
    public static final String CLOSE_SYMBOL_KEY = "close_fn";
    public static final String MD5_CHECKSUM = "md5";
    public static final String INIT_KEY = "init_fn";
    public static final String UPDATE_KEY = "update_fn";
    public static final String MERGE_KEY = "merge_fn";
    public static final String SERIALIZE_KEY = "serialize_fn";
    public static final String FINALIZE_KEY = "finalize_fn";
    public static final String GET_VALUE_KEY = "get_value_fn";
    public static final String REMOVE_KEY = "remove_fn";
    public static final String OBJECT_TYPE_KEY = "object_type";
    public static final String OBJECT_TYPE_STARROCKS_JAR = "StarrocksJar";
    public static final String PREPARE_METHOD_NAME = "prepare";
    public static final String CLOSE_METHOD_NAME = "close";
    public static final String EVAL_METHOD_NAME = "evaluate";


    private final FunctionName functionName;
    private final boolean isAggregate;
    private final FunctionArgsDef argsDef;
    private final TypeDef returnType;
    private TypeDef intermediateType;
    private final Map<String, String> properties;
    private boolean isStarrocksJar = false;

    // needed item set after analyzed
    private String objectFile;
    private Function function;
    private String checksum;
    private Class udfClass;

    private static final ImmutableMap<PrimitiveType, Class> PrimitiveTypeToJavaClassType = new ImmutableMap.Builder<PrimitiveType, Class>()
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


    public CreateFunctionStmt(boolean isAggregate, FunctionName functionName, FunctionArgsDef argsDef,
                              TypeDef returnType, TypeDef intermediateType, Map<String, String> properties) {
        this.functionName = functionName;
        this.isAggregate = isAggregate;
        this.argsDef = argsDef;
        this.returnType = returnType;
        this.intermediateType = intermediateType;
        if (properties == null) {
            this.properties = ImmutableSortedMap.of();
        } else {
            this.properties = ImmutableSortedMap.copyOf(properties, String.CASE_INSENSITIVE_ORDER);
        }
    }

    public FunctionName getFunctionName() {
        return functionName;
    }

    public Function getFunction() {
        return function;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        analyzeCommon(analyzer);
        // check
        if (isAggregate) {
            analyzeUda();
        } else {
            if (isStarrocksJar) {
                analyzeStarrocksJarUdf();
            } else {
                analyzeUdf();
            }
        }
    }

    private void analyzeCommon(Analyzer analyzer) throws AnalysisException {
        // check function name
        functionName.analyze(analyzer);

        // check operation privilege
        if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
        // check argument
        argsDef.analyze(analyzer);

        returnType.analyze(analyzer);
        if (intermediateType != null) {
            intermediateType.analyze(analyzer);
        } else {
            intermediateType = returnType;
        }

        String lang = properties.get(OBJECT_TYPE_KEY);
        if (OBJECT_TYPE_STARROCKS_JAR.equals(lang)) {
            isStarrocksJar = true;
        }

        objectFile = properties.get(OBJECT_FILE_KEY);
        if (Strings.isNullOrEmpty(objectFile)) {
            throw new AnalysisException("No 'object_file' in properties");
        }
        try {
            computeObjectChecksum();
        } catch (IOException | NoSuchAlgorithmException e) {
            throw new AnalysisException("cannot to compute object's checksum");
        }

        String md5sum = properties.get(MD5_CHECKSUM);
        if (md5sum != null && !md5sum.equalsIgnoreCase(checksum)) {
            throw new AnalysisException("library's checksum is not equal with input, checksum=" + checksum);
        }

        if (isStarrocksJar) {
            analyzeUdfClassInStarrocksJar();
        }
    }

    private void analyzeUdfClassInStarrocksJar() throws AnalysisException {
        String symbol = properties.get(SYMBOL_KEY);
        if (Strings.isNullOrEmpty(symbol)) {
            throw new AnalysisException("No '" + SYMBOL_KEY + "' in properties");
        }

        try {
            URL[] urls = {new URL("jar:" + objectFile + "!/")};
            URLClassLoader cl = URLClassLoader.newInstance(urls);
            udfClass = cl.loadClass(symbol);
        } catch (MalformedURLException e) {
            throw new AnalysisException("failed to load object_file: " + objectFile);
        } catch (ClassNotFoundException e) {
            throw new AnalysisException("class '" + symbol + "' not found in object_file :" + objectFile);
        }
    }

    private void computeObjectChecksum() throws IOException, NoSuchAlgorithmException {
        if (FeConstants.runningUnitTest) {
            // skip checking checksum when running ut
            checksum = "";
            return;
        }
        URL url = new URL(objectFile);
        URLConnection urlConnection = url.openConnection();
        InputStream inputStream = urlConnection.getInputStream();

        MessageDigest digest = MessageDigest.getInstance("MD5");
        byte[] buf = new byte[4096];
        int bytesRead = 0;
        do {
            bytesRead = inputStream.read(buf);
            if (bytesRead < 0) {
                break;
            }
            digest.update(buf, 0, bytesRead);
        } while (true);

        checksum = Hex.encodeHexString(digest.digest());
    }

    private void analyzeUda() throws AnalysisException {
        AggregateFunction.AggregateFunctionBuilder builder =
                AggregateFunction.AggregateFunctionBuilder.createUdfBuilder();

        builder.name(functionName).argsType(argsDef.getArgTypes()).retType(returnType.getType()).
                hasVarArgs(argsDef.isVariadic()).intermediateType(intermediateType.getType()).objectFile(objectFile);
        String initFnSymbol = properties.get(INIT_KEY);
        if (initFnSymbol == null) {
            throw new AnalysisException("No 'init_fn' in properties");
        }
        String updateFnSymbol = properties.get(UPDATE_KEY);
        if (updateFnSymbol == null) {
            throw new AnalysisException("No 'update_fn' in properties");
        }
        String mergeFnSymbol = properties.get(MERGE_KEY);
        if (mergeFnSymbol == null) {
            throw new AnalysisException("No 'merge_fn' in properties");
        }
        function = builder.build();
        function.setChecksum(checksum);
    }

    private void analyzeUdf() throws AnalysisException {
        String symbol = properties.get(SYMBOL_KEY);
        if (Strings.isNullOrEmpty(symbol)) {
            throw new AnalysisException("No 'symbol' in properties");
        }
        String prepareFnSymbol = properties.get(PREPARE_SYMBOL_KEY);
        String closeFnSymbol = properties.get(CLOSE_SYMBOL_KEY);
        function = ScalarFunction.createUdf(
                functionName, argsDef.getArgTypes(),
                returnType.getType(), argsDef.isVariadic(), TFunctionBinaryType.HIVE,
                objectFile, symbol, prepareFnSymbol, closeFnSymbol);
        function.setChecksum(checksum);
    }

    private void checkStarrocksJarUdfType(Type type, Class ptype, String pname) throws AnalysisException {
        if (!(type instanceof ScalarType)) {
            throw new AnalysisException("UDF does not support non-scalar type: " + type);
        }
        ScalarType scalarType = (ScalarType) type;
        Class cls = PrimitiveTypeToJavaClassType.get(scalarType.getPrimitiveType());
        if (cls == null) {
            throw new AnalysisException("UDF does not support type: " + scalarType);
        }
        if (!cls.equals(ptype)) {
            throw new AnalysisException(String.format("UDF %s[%s] type does not match %s", pname,
                    ptype.getCanonicalName(), cls.getCanonicalName()));
        }
    }

    private void checkStarrocksJarUdfMethod(Method method) throws AnalysisException {
        String name = method.getName();
        boolean inspected = true;
        if (PREPARE_METHOD_NAME.equals(name) || EVAL_METHOD_NAME.equals(name)) {
            Class retType = method.getReturnType();
            checkStarrocksJarUdfType(returnType.getType(), retType, "Return");
            if (method.getParameters().length != argsDef.getArgTypes().length) {
                throw new AnalysisException(String.format("UDF '%s' parameter count does not match", name));
            }
            for (int i = 0; i < method.getParameters().length; i++) {
                Parameter p = method.getParameters()[i];
                checkStarrocksJarUdfType(argsDef.getArgTypes()[i], p.getType(), p.getName());
            }
        } else if (CLOSE_METHOD_NAME.equals(name)) {
            Class retType = method.getReturnType();
            if (!retType.equals(void.class)) {
                throw new AnalysisException(String.format("UDF '%s' return type should be void", CLOSE_METHOD_NAME));
            }
            if (method.getParameters().length != 0) {
                throw new AnalysisException(String.format("UDF '%s' should have zero parameter", CLOSE_METHOD_NAME));
            }
        } else {
            inspected = false;
        }
        if (inspected) {
            if (Modifier.isStatic(method.getModifiers())) {
                throw new AnalysisException(String.format("UDF '%s' should be non-static method", name));
            }
        }
    }

    private void checkStarrocksJarUdfClass() throws AnalysisException {
        boolean hasEvalMethod = false;
        for (Method m : udfClass.getMethods()) {
            if (EVAL_METHOD_NAME.equals(m.getName())) {
                hasEvalMethod = true;
            }
            checkStarrocksJarUdfMethod(m);
        }
        if (!hasEvalMethod) {
            throw new AnalysisException(String.format("UDF should have '%s'", EVAL_METHOD_NAME));
        }
    }

    private void analyzeStarrocksJarUdf() throws AnalysisException {
        checkStarrocksJarUdfClass();
        function = ScalarFunction.createUdf(
                functionName, argsDef.getArgTypes(),
                returnType.getType(), argsDef.isVariadic(), TFunctionBinaryType.SRJAR,
                objectFile, udfClass.getCanonicalName(), "", "");
        function.setChecksum(checksum);
    }


    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("CREATE ");
        if (isAggregate) {
            stringBuilder.append("AGGREGATE ");
        }
        stringBuilder.append("FUNCTION ");
        stringBuilder.append(functionName.toString());
        stringBuilder.append(argsDef.toSql());
        stringBuilder.append(" RETURNS ");
        stringBuilder.append(returnType.toString());
        if (properties.size() > 0) {
            stringBuilder.append(" PROPERTIES (");
            int i = 0;
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                if (i != 0) {
                    stringBuilder.append(", ");
                }
                stringBuilder.append('"').append(entry.getKey()).append('"');
                stringBuilder.append("=");
                stringBuilder.append('"').append(entry.getValue()).append('"');
                i++;
            }
            stringBuilder.append(")");

        }
        return stringBuilder.toString();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }
}
