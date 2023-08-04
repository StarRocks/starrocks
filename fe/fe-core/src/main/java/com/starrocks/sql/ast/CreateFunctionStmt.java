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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.TypeDef;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.TableFunction;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.parser.NodePosition;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    public static final String WINDOW_UPDATE_METHOD_NAME = "windowUpdate";
    public static final String IS_ANALYTIC_NAME = "analytic";
    public static final String PROCESS_METHOD_NAME = "process";

    private final FunctionName functionName;
    private final boolean isAggregate;
    private final boolean isTable;
    private final FunctionArgsDef argsDef;
    private final TypeDef returnType;
    private TypeDef intermediateType;
    private final Map<String, String> properties;
    private boolean isStarrocksJar = false;
    private boolean isAnalyticFn = false;

    // needed item set after analyzed
    private String objectFile;
    private Function function;
    private String checksum;

    private static final ImmutableMap<PrimitiveType, Class> PRIMITIVE_TYPE_TO_JAVA_CLASS_TYPE =
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
            Class cls = PRIMITIVE_TYPE_TO_JAVA_CLASS_TYPE.get(scalarType.getPrimitiveType());
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

    private UDFInternalClass mainClass;
    private UDFInternalClass udafStateClass;

    public CreateFunctionStmt(String functionType, FunctionName functionName, FunctionArgsDef argsDef,
                              TypeDef returnType, TypeDef intermediateType, Map<String, String> properties) {
        this(functionType, functionName, argsDef, returnType, intermediateType, properties, NodePosition.ZERO);
    }

    public CreateFunctionStmt(String functionType, FunctionName functionName, FunctionArgsDef argsDef,
                              TypeDef returnType, TypeDef intermediateType, Map<String, String> properties,
                              NodePosition pos) {
        super(pos);
        this.functionName = functionName;
        this.isAggregate = functionType.equalsIgnoreCase("AGGREGATE");
        this.isTable = functionType.equalsIgnoreCase("TABLE");
        this.argsDef = argsDef;
        this.returnType = returnType;
        this.intermediateType = intermediateType;
        if (properties == null) {
            this.properties = ImmutableSortedMap.of();
        } else {
            Map<String, String> lowerCaseProperties = new HashMap<>();
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                lowerCaseProperties.put(entry.getKey().toLowerCase(), entry.getValue());
            }

            this.properties = ImmutableSortedMap.copyOf(lowerCaseProperties, String.CASE_INSENSITIVE_ORDER);
        }
        this.mainClass = new UDFInternalClass();
        if (isAggregate) {
            this.udafStateClass = new UDFInternalClass();
        }
    }

    public FunctionName getFunctionName() {
        return functionName;
    }

    public Function getFunction() {
        return function;
    }

    public void setFunction(Function function) {
        this.function = function;
    }

    public void analyze(ConnectContext context) throws AnalysisException {
        if (!Config.enable_udf) {
            throw new AnalysisException(
                    "UDF is not enabled in FE, please configure enable_udf=true in fe/conf/fe.conf or ");
        }
        analyzeCommon(context.getDatabase());
        Preconditions.checkArgument(isStarrocksJar);
        analyzeUdfClassInStarrocksJar();
        if (isAggregate) {
            analyzeStarrocksJarUdaf();
        } else if (isTable) {
            analyzeStarrocksJarUdtf();
        } else {
            analyzeStarrocksJarUdf();
        }
    }

    private void analyzeCommon(String defaultDb) throws AnalysisException {
        // check function name
        functionName.analyze(defaultDb);

        // check argument
        argsDef.analyze();
        returnType.analyze();

        intermediateType = TypeDef.createVarchar(ScalarType.MAX_VARCHAR_LENGTH);

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
            throw new AnalysisException("cannot to compute object's checksum", e);
        }

        String md5sum = properties.get(MD5_CHECKSUM);
        if (md5sum != null && !md5sum.equalsIgnoreCase(checksum)) {
            throw new AnalysisException("library's checksum is not equal with input, checksum=" + checksum);
        }
    }

    private void analyzeUdfClassInStarrocksJar() throws AnalysisException {
        String className = properties.get(SYMBOL_KEY);
        if (Strings.isNullOrEmpty(className)) {
            throw new AnalysisException("No '" + SYMBOL_KEY + "' in properties");
        }

        try {
            URL[] urls = {new URL("jar:" + objectFile + "!/")};
            try (URLClassLoader classLoader = URLClassLoader.newInstance(urls)) {
                mainClass.setClazz(classLoader.loadClass(className));

                if (isAggregate) {
                    String stateClassName = className + "$" + STATE_CLASS_NAME;
                    udafStateClass.setClazz(classLoader.loadClass(stateClassName));
                }

                mainClass.collectMethods();
                if (isAggregate) {
                    udafStateClass.collectMethods();
                }

            } catch (IOException e) {
                throw new AnalysisException("Failed to load object_file: " + objectFile);
            } catch (ClassNotFoundException e) {
                throw new AnalysisException("Class '" + className + "' not found in object_file :" + objectFile);
            }
        } catch (MalformedURLException e) {
            throw new AnalysisException("Object file is invalid: " + objectFile);
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
            Method method = mainClass.getMethod(EVAL_METHOD_NAME, true);
            mainClass.checkMethodNonStaticAndPublic(method);
            mainClass.checkArgumentCount(method, argsDef.getArgTypes().length);
            mainClass.checkReturnUdfType(method, returnType.getType());
            for (int i = 0; i < method.getParameters().length; i++) {
                Parameter p = method.getParameters()[i];
                mainClass.checkUdfType(method, argsDef.getArgTypes()[i], p.getType(), p.getName());
            }
        }
    }

    private void analyzeStarrocksJarUdf() throws AnalysisException {
        checkStarrocksJarUdfClass();
        function = ScalarFunction.createUdf(
                functionName, argsDef.getArgTypes(),
                returnType.getType(), argsDef.isVariadic(), TFunctionBinaryType.SRJAR,
                objectFile, mainClass.getCanonicalName(), "", "");
        function.setChecksum(checksum);
    }

    private void checkStarrocksJarUdafStateClass() throws AnalysisException {
        // Check internal State class
        // should be public & static.
        Class stateClass = udafStateClass.clazz;
        if (!Modifier.isPublic(stateClass.getModifiers()) ||
                !Modifier.isStatic(stateClass.getModifiers())) {
            throw new AnalysisException(
                    String.format("UDAF '%s' should have one public & static 'State' class",
                            mainClass.getCanonicalName()));
        }
        {
            // long serializeLength();
            Method method = udafStateClass.getMethod(SERIALIZE_LENGTH_METHOD_NAME, true);
            udafStateClass.checkMethodNonStaticAndPublic(method);
            udafStateClass.checkReturnJavaType(method, int.class);
            udafStateClass.checkArgumentCount(method, 0);
        }
    }

    private void checkStarrocksJarUdafClass() throws AnalysisException {
        {
            // State create()
            Method method = mainClass.getMethod(CREATE_METHOD_NAME, true);
            mainClass.checkMethodNonStaticAndPublic(method);
            mainClass.checkReturnJavaType(method, udafStateClass.clazz);
            mainClass.checkArgumentCount(method, 0);
        }
        {
            // void destroy(State);
            Method method = mainClass.getMethod(DESTROY_METHOD_NAME, true);
            mainClass.checkMethodNonStaticAndPublic(method);
            mainClass.checkReturnJavaType(method, void.class);
            mainClass.checkArgumentCount(method, 1);
            mainClass.checkParamJavaType(method, udafStateClass.clazz, method.getParameters()[0]);
        }
        {
            // void update(State, ....)
            Method method = mainClass.getMethod(UPDATE_METHOD_NAME, true);
            mainClass.checkMethodNonStaticAndPublic(method);
            mainClass.checkReturnJavaType(method, void.class);
            mainClass.checkArgumentCount(method, argsDef.getArgTypes().length + 1);
            mainClass.checkParamJavaType(method, udafStateClass.clazz, method.getParameters()[0]);
            for (int i = 0; i < argsDef.getArgTypes().length; i++) {
                mainClass.checkParamUdfType(method, argsDef.getArgTypes()[i], method.getParameters()[i + 1]);
            }
        }
        {
            // void serialize(State, java.nio.ByteBuffer)
            Method method = mainClass.getMethod(SERIALIZE_METHOD_NAME, true);
            mainClass.checkMethodNonStaticAndPublic(method);
            mainClass.checkReturnJavaType(method, void.class);
            mainClass.checkArgumentCount(method, 2);
            mainClass.checkParamJavaType(method, udafStateClass.clazz, method.getParameters()[0]);
            mainClass.checkParamJavaType(method, java.nio.ByteBuffer.class, method.getParameters()[1]);
        }
        {
            // void merge(State, java.nio.ByteBuffer)
            Method method = mainClass.getMethod(MERGE_METHOD_NAME, true);
            mainClass.checkMethodNonStaticAndPublic(method);
            mainClass.checkReturnJavaType(method, void.class);
            mainClass.checkArgumentCount(method, 2);
            mainClass.checkParamJavaType(method, udafStateClass.clazz, method.getParameters()[0]);
            mainClass.checkParamJavaType(method, java.nio.ByteBuffer.class, method.getParameters()[1]);
        }
        {
            // RETURN_TYPE finalize(State);
            Method method = mainClass.getMethod(FINALIZE_METHOD_NAME, true);
            mainClass.checkMethodNonStaticAndPublic(method);
            mainClass.checkReturnUdfType(method, returnType.getType());
            mainClass.checkArgumentCount(method, 1);
            mainClass.checkParamJavaType(method, udafStateClass.clazz, method.getParameters()[0]);
        }
        if (isAnalyticFn) {
            {
                Method method = mainClass.getMethod(WINDOW_UPDATE_METHOD_NAME, true);
                mainClass.checkMethodNonStaticAndPublic(method);
            }
        }
    }

    private void analyzeStarrocksJarUdtf() throws AnalysisException {
        {
            // TYPE[] process(INPUT)
            Method method = mainClass.getMethod(PROCESS_METHOD_NAME, true);
            mainClass.checkMethodNonStaticAndPublic(method);
            mainClass.checkArgumentCount(method, argsDef.getArgTypes().length);
            for (int i = 0; i < method.getParameters().length; i++) {
                Parameter p = method.getParameters()[i];
                mainClass.checkUdfType(method, argsDef.getArgTypes()[i], p.getType(), p.getName());
            }
        }
        final List<Type> argList = Arrays.stream(argsDef.getArgTypes()).collect(Collectors.toList());
        TableFunction tableFunction = new TableFunction(functionName,
                Lists.newArrayList(functionName.getFunction()),
                argList, Lists.newArrayList(returnType.getType()));
        tableFunction.setBinaryType(TFunctionBinaryType.SRJAR);
        tableFunction.setChecksum(checksum);
        tableFunction.setLocation(new HdfsURI(objectFile));
        tableFunction.setSymbolName(mainClass.getCanonicalName());
        function = tableFunction;
    }

    private void analyzeStarrocksJarUdaf() throws AnalysisException {
        isAnalyticFn = "true".equalsIgnoreCase(properties.get(IS_ANALYTIC_NAME));
        checkStarrocksJarUdafStateClass();
        checkStarrocksJarUdafClass();
        AggregateFunction.AggregateFunctionBuilder builder =
                AggregateFunction.AggregateFunctionBuilder.createUdfBuilder(TFunctionBinaryType.SRJAR);
        builder.name(functionName).argsType(argsDef.getArgTypes()).retType(returnType.getType()).
                hasVarArgs(argsDef.isVariadic()).intermediateType(intermediateType.getType()).objectFile(objectFile)
                .isAnalyticFn(isAnalyticFn)
                .symbolName(mainClass.getCanonicalName());
        function = builder.build();
        function.setChecksum(checksum);
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateFunctionStatement(this, context);
    }
}
