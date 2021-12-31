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

import com.google.common.base.Preconditions;
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
import java.util.HashMap;
import java.util.Map;

// create a user define function
public class CreateFunctionStmt extends DdlStmt {
    public static final String FILE_KEY = "file";
    public static final String SYMBOL_KEY = "symbol";
    public static final String MD5_CHECKSUM = "md5";
    public static final String TYPE_KEY = "type";
    public static final String TYPE_STARROCKS_JAR = "StarrocksJar";
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

    private static final ImmutableMap<PrimitiveType, Class> PrimitiveTypeToJavaClassType =
            new ImmutableMap.Builder<PrimitiveType, Class>()
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

    private static class UDFInternalClass {
        public Class clazz = null;
        public Map<String, Method> methods = null;

        public String getCanonicalName() {
            return clazz.getCanonicalName();
        }

        public void setClazz(Class clazz) {
            this.clazz = clazz;
        }

        public void collectMethods() throws AnalysisException {
            methods = new HashMap<>();
            for (Method m : clazz.getMethods()) {
                if (!m.getDeclaringClass().equals(clazz)) {
                    continue;
                }
                String name = m.getName();
                if (methods.containsKey(name)) {
                    throw new AnalysisException(String.format(
                            "UDF class '%s' has more than one method '%s' ", clazz.getCanonicalName(), name));
                }
                methods.put(name, m);
            }
        }

        public Method getMethod(String name, boolean required) throws AnalysisException {
            Method m = methods.get(name);
            if (m == null && required) {
                throw new AnalysisException(
                        String.format("UDF class '%s must have method '%s'", clazz.getCanonicalName(), name));
            }
            return m;
        }

        public void checkMethodNonStaticAndPublic(Method method) throws AnalysisException {
            if (Modifier.isStatic(method.getModifiers())) {
                throw new AnalysisException(
                        String.format("UDF class '%s' method '%s' should be non-static", clazz.getCanonicalName(),
                                method.getName()));
            }
            if (!Modifier.isPublic(method.getModifiers())) {
                throw new AnalysisException(
                        String.format("UDF class '%s' method '%s' should be public", clazz.getCanonicalName(),
                                method.getName()));
            }
        }

        private void checkParamJavaType(Method method, Class expType, Parameter p) throws AnalysisException {
            checkJavaType(method, expType, p.getType(), p.getName());
        }

        private void checkReturnJavaType(Method method, Class expType) throws AnalysisException {
            checkJavaType(method, expType, method.getReturnType(), RETURN_FIELD_NAME);
        }

        private void checkJavaType(Method method, Class expType, Class ptype, String pname)
                throws AnalysisException {
            if (!expType.equals(ptype)) {
                throw new AnalysisException(
                        String.format("UDF class '%s' method '%s' parameter %s[%s] expect type %s",
                                clazz.getCanonicalName(), method.getName(), pname, ptype.getCanonicalName(),
                                expType.getCanonicalName()));
            }
        }

        private void checkArgumentCount(Method method, int argumentCount)
                throws AnalysisException {
            if (method.getParameters().length != argumentCount) {
                throw new AnalysisException(
                        String.format("UDF class '%s' method '%s' expect argument count %d",
                                clazz.getCanonicalName(), method.getName(), argumentCount));
            }
        }

        private void checkParamUdfType(Method method, Type expType, Parameter p) throws AnalysisException {
            checkUdfType(method, expType, p.getType(), p.getName());
        }

        private void checkReturnUdfType(Method method, Type expType) throws AnalysisException {
            checkUdfType(method, expType, method.getReturnType(), RETURN_FIELD_NAME);
        }

        private void checkUdfType(Method method, Type expType, Class ptype, String pname)
                throws AnalysisException {
            if (!(expType instanceof ScalarType)) {
                throw new AnalysisException(
                        String.format("UDF class '%s' method '%s' does not support non-scalar type '%s'",
                                clazz.getCanonicalName(), method.getName(), expType));
            }
            ScalarType scalarType = (ScalarType) expType;
            Class cls = PrimitiveTypeToJavaClassType.get(scalarType.getPrimitiveType());
            if (cls == null) {
                throw new AnalysisException(
                        String.format("UDF class '%s' method '%s' does not support type '%s'",
                                clazz.getCanonicalName(), method.getName(), scalarType));
            }
            if (!cls.equals(ptype)) {
                throw new AnalysisException(
                        String.format("UDF class '%s' method '%s' parameter %s[%s] type does not match %s",
                                clazz.getCanonicalName(), method.getName(), pname, ptype.getCanonicalName(),
                                cls.getCanonicalName()));
            }
        }
    }

    private UDFInternalClass udfClass;
    private UDFInternalClass udfStateClass;

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
        this.udfClass = new UDFInternalClass();
        if (isAggregate) {
            this.udfStateClass = new UDFInternalClass();
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
        Preconditions.checkArgument(isStarrocksJar);
        analyzeUdfClassInStarrocksJar();
        if (isAggregate) {
            analyzeStarrocksJarUdaf();
        } else {
            analyzeStarrocksJarUdf();
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

        String type = properties.get(TYPE_KEY);
        if (TYPE_STARROCKS_JAR.equals(type)) {
            isStarrocksJar = true;
        }

        objectFile = properties.get(FILE_KEY);
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
    }

    private void analyzeUdfClassInStarrocksJar() throws AnalysisException {
        String class_name = properties.get(SYMBOL_KEY);
        if (Strings.isNullOrEmpty(class_name)) {
            throw new AnalysisException("No '" + SYMBOL_KEY + "' in properties");
        }

        try {
            URL[] urls = {new URL("jar:" + objectFile + "!/")};
            try (URLClassLoader classLoader = URLClassLoader.newInstance(urls)) {
                udfClass.setClazz(classLoader.loadClass(class_name));

                if (isAggregate) {
                    String state_class_name = class_name + "$" + STATE_CLASS_NAME;
                    udfStateClass.setClazz(classLoader.loadClass(state_class_name));
                }

            } catch (IOException e) {
                throw new AnalysisException("Failed to load object_file: " + objectFile);
            } catch (ClassNotFoundException e) {
                throw new AnalysisException("Class '" + class_name + "' not found in object_file :" + objectFile);
            }
        } catch (MalformedURLException e) {
            throw new AnalysisException("Object file is invalid: " + objectFile);
        }

        udfClass.collectMethods();
        if (isAggregate) {
            udfStateClass.collectMethods();
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

    private void checkStarrocksJarUdfClass() throws AnalysisException {
        {
            // RETURN_TYPE evaluate(...)
            Method method = udfClass.getMethod(EVAL_METHOD_NAME, true);
            udfClass.checkMethodNonStaticAndPublic(method);
            udfClass.checkArgumentCount(method, argsDef.getArgTypes().length);
            udfClass.checkReturnUdfType(method, returnType.getType());
            for (int i = 0; i < method.getParameters().length; i++) {
                Parameter p = method.getParameters()[i];
                udfClass.checkUdfType(method, argsDef.getArgTypes()[i], p.getType(), p.getName());
            }
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

    private void checkStarrocksJarUdafStateClass() throws AnalysisException {
        // Check internal State class
        // should be public & static.
        Class stateClass = udfStateClass.clazz;
        if (!Modifier.isPublic(stateClass.getModifiers()) ||
                !Modifier.isStatic(stateClass.getModifiers())) {
            throw new AnalysisException(
                    String.format("UDAF '%s' should have one public & static 'State' class",
                            udfClass.getCanonicalName()));
        }
        {
            // long serializeLength();
            Method method = udfStateClass.getMethod(SERIALIZE_LENGTH_METHOD_NAME, true);
            udfStateClass.checkMethodNonStaticAndPublic(method);
            udfStateClass.checkReturnJavaType(method, long.class);
            udfStateClass.checkArgumentCount(method, 0);
        }
    }

    private void checkStarrocksJarUdafClass() throws AnalysisException {
        {
            // State create()
            Method method = udfClass.getMethod(CREATE_METHOD_NAME, true);
            udfClass.checkMethodNonStaticAndPublic(method);
            udfClass.checkReturnJavaType(method, udfStateClass.clazz);
            udfClass.checkArgumentCount(method, 0);
        }
        {
            // void destroy(State);
            Method method = udfClass.getMethod(DESTROY_METHOD_NAME, true);
            udfClass.checkMethodNonStaticAndPublic(method);
            udfClass.checkReturnJavaType(method, void.class);
            udfClass.checkArgumentCount(method, 1);
            udfClass.checkParamJavaType(method, udfStateClass.clazz, method.getParameters()[0]);
        }
        {
            // void update(State, ....)
            Method method = udfClass.getMethod(UPDATE_METHOD_NAME, true);
            udfClass.checkMethodNonStaticAndPublic(method);
            udfClass.checkReturnJavaType(method, void.class);
            udfClass.checkArgumentCount(method, argsDef.getArgTypes().length + 1);
            udfClass.checkParamJavaType(method, udfStateClass.clazz, method.getParameters()[0]);
            for (int i = 0; i < argsDef.getArgTypes().length; i++) {
                udfClass.checkParamUdfType(method, argsDef.getArgTypes()[i], method.getParameters()[i + 1]);
            }
        }
        {
            // void serialize(State, java.nio.ByteBuffer)
            Method method = udfClass.getMethod(SERIALIZE_METHOD_NAME, true);
            udfClass.checkMethodNonStaticAndPublic(method);
            udfClass.checkReturnJavaType(method, void.class);
            udfClass.checkArgumentCount(method, 2);
            udfClass.checkParamJavaType(method, udfStateClass.clazz, method.getParameters()[0]);
            udfClass.checkParamJavaType(method, java.nio.ByteBuffer.class, method.getParameters()[1]);
        }
        {
            // void merge(State, java.nio.ByteBuffer)
            Method method = udfClass.getMethod(MERGE_METHOD_NAME, true);
            udfClass.checkMethodNonStaticAndPublic(method);
            udfClass.checkReturnJavaType(method, void.class);
            udfClass.checkArgumentCount(method, 2);
            udfClass.checkParamJavaType(method, udfStateClass.clazz, method.getParameters()[0]);
            udfClass.checkParamJavaType(method, java.nio.ByteBuffer.class, method.getParameters()[1]);
        }
        {
            // RETURN_TYPE finalize(State);
            Method method = udfClass.getMethod(FINALIZE_METHOD_NAME, true);
            udfClass.checkMethodNonStaticAndPublic(method);
            udfClass.checkReturnUdfType(method, returnType.getType());
            udfClass.checkArgumentCount(method, 1);
            udfClass.checkParamJavaType(method, udfStateClass.clazz, method.getParameters()[0]);
        }
    }

    private void analyzeStarrocksJarUdaf() throws AnalysisException {
        checkStarrocksJarUdafStateClass();
        checkStarrocksJarUdafClass();
        AggregateFunction.AggregateFunctionBuilder builder =
                AggregateFunction.AggregateFunctionBuilder.createUdfBuilder(TFunctionBinaryType.SRJAR);
        builder.name(functionName).argsType(argsDef.getArgTypes()).retType(returnType.getType()).
                hasVarArgs(argsDef.isVariadic()).intermediateType(intermediateType.getType()).objectFile(objectFile)
                .symbolName(udfClass.getCanonicalName());
        function = builder.build();
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
