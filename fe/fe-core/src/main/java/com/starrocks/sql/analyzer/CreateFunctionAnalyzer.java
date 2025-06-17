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

package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.TypeDef;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.TableFunction;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.CreateFunctionStmt;
import com.starrocks.sql.ast.FunctionArgsDef;
import com.starrocks.sql.ast.HdfsURI;
import com.starrocks.thrift.TFunctionBinaryType;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Permission;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CreateFunctionAnalyzer {
    public void analyze(CreateFunctionStmt stmt, ConnectContext context) {
        if (!Config.enable_udf) {
            throw new SemanticException(
                    "UDF is not enabled in FE, please configure enable_udf=true in fe/conf/fe.conf");
        }
        analyzeCommon(stmt, context);
        String langType = stmt.getLangType();

        if (CreateFunctionStmt.TYPE_STARROCKS_JAR.equalsIgnoreCase(langType)) {
            String checksum = computeMd5(stmt);
            analyzeJavaUDFClass(stmt, checksum);
        } else if (CreateFunctionStmt.TYPE_STARROCKS_PYTHON.equalsIgnoreCase(langType)) {
            analyzePython(stmt);
        } else {
            ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_OBJECT, "unknown lang type");
        }
        // build function
    }

    private void analyzeCommon(CreateFunctionStmt stmt, ConnectContext context) {
        FunctionName functionName = stmt.getFunctionName();
        functionName.analyze(context.getDatabase());
        FunctionArgsDef argsDef = stmt.getArgsDef();
        TypeDef returnType = stmt.getReturnType();
        // check argument
        argsDef.analyze();
        returnType.analyze();
    }

    public String computeMd5(CreateFunctionStmt stmt) {
        String checksum = "";
        if (FeConstants.runningUnitTest) {
            // skip checking checksum when running ut
            checksum = "";
            return checksum;
        }

        Map<String, String> properties = stmt.getProperties();

        String objectFile = properties.get(CreateFunctionStmt.FILE_KEY);
        if (Strings.isNullOrEmpty(objectFile)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "No 'object_file' in properties");
        }

        try {
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
        } catch (IOException | NoSuchAlgorithmException e) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "cannot to compute object's checksum", e);
        }

        String md5sum = properties.get(CreateFunctionStmt.MD5_CHECKSUM);
        if (md5sum != null && !md5sum.equalsIgnoreCase(checksum)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                    "library's checksum is not equal with input, checksum=" + checksum);
        }

        return checksum;
    }

    private void analyzeJavaUDFClass(CreateFunctionStmt stmt, String checksum) {
        Map<String, String> properties = stmt.getProperties();
        String className = properties.get(CreateFunctionStmt.SYMBOL_KEY);
        if (Strings.isNullOrEmpty(className)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                    "No '" + CreateFunctionStmt.SYMBOL_KEY + "' in properties");
        }
        String objectFile = properties.get(CreateFunctionStmt.FILE_KEY);

        JavaUDFInternalClass handleClass = new JavaUDFInternalClass();
        JavaUDFInternalClass stateClass = new JavaUDFInternalClass();

        try {
            System.setSecurityManager(new UDFSecurityManager(UDFInternalClassLoader.class));
            try (URLClassLoader classLoader = new UDFInternalClassLoader(objectFile)) {
                handleClass.setClazz(classLoader.loadClass(className));
                handleClass.collectMethods();

                if (stmt.isAggregate()) {
                    String stateClassName = className + "$" + CreateFunctionStmt.STATE_CLASS_NAME;
                    stateClass.setClazz(classLoader.loadClass(stateClassName));
                    stateClass.collectMethods();
                }

            } catch (IOException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_OBJECT,
                        "Failed to load object_file: " + objectFile, e);
            } catch (ClassNotFoundException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_OBJECT,
                        "Class '" + className + "' not found in object_file :" + objectFile, e);
            } catch (Exception e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_OBJECT,
                        "other exception when load class. exception:", e);
            }
        } finally {
            System.setSecurityManager(null);
        }

        Function createdFunction = null;
        if (stmt.isScalar()) {
            createdFunction = analyzeStarrocksJarUdf(stmt, checksum, handleClass);
        } else if (stmt.isAggregate()) {
            createdFunction = analyzeStarrocksJarUdaf(stmt, checksum, handleClass, stateClass);
        } else {
            createdFunction = analyzeStarrocksJarUdtf(stmt, checksum, handleClass);
        }
        
        stmt.setFunction(createdFunction);
    }

    private void checkStarrocksJarUdfClass(CreateFunctionStmt stmt, JavaUDFInternalClass mainClass) {
        FunctionArgsDef argsDef = stmt.getArgsDef();
        TypeDef returnType = stmt.getReturnType();
        // RETURN_TYPE evaluate(...)
        Method method = mainClass.getMethod(CreateFunctionStmt.EVAL_METHOD_NAME, true);
        mainClass.checkMethodNonStaticAndPublic(method);
        mainClass.checkArgumentCount(method, argsDef.getArgTypes().length);
        mainClass.checkScalarReturnUdfType(method, returnType.getType());
        for (int i = 0; i < method.getParameters().length; i++) {
            Parameter p = method.getParameters()[i];
            mainClass.checkScalarUdfType(method, argsDef.getArgTypes()[i], p.getType(), p.getName());
        }
    }

    private Function analyzeStarrocksJarUdf(CreateFunctionStmt stmt, String checksum,
                                               JavaUDFInternalClass handleClass) {
        checkStarrocksJarUdfClass(stmt, handleClass);

        FunctionName functionName = stmt.getFunctionName();
        FunctionArgsDef argsDef = stmt.getArgsDef();
        TypeDef returnType = stmt.getReturnType();
        String objectFile = stmt.getProperties().get(CreateFunctionStmt.FILE_KEY);
        String isolation = stmt.getProperties().get(CreateFunctionStmt.ISOLATION_KEY);

        Function function = ScalarFunction.createUdf(
                functionName, argsDef.getArgTypes(),
                returnType.getType(), argsDef.isVariadic(), TFunctionBinaryType.SRJAR,
                objectFile, handleClass.getCanonicalName(), "", "", !"shared".equalsIgnoreCase(isolation));
        function.setChecksum(checksum);
        return function;
    }

    private void checkStarrocksJarUdafStateClass(CreateFunctionStmt stmt, JavaUDFInternalClass mainClass,
                                                 JavaUDFInternalClass udafStateClass) {
        // Check internal State class
        // should be public & static.
        Class<?> stateClass = udafStateClass.clazz;
        if (!Modifier.isPublic(stateClass.getModifiers()) ||
                !Modifier.isStatic(stateClass.getModifiers())) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                    String.format("UDAF '%s' should have one public & static 'State' class",
                            mainClass.getCanonicalName()));
        }
        {
            // long serializeLength();
            Method method = udafStateClass.getMethod(CreateFunctionStmt.SERIALIZE_LENGTH_METHOD_NAME, true);
            udafStateClass.checkMethodNonStaticAndPublic(method);
            udafStateClass.checkReturnJavaType(method, int.class);
            udafStateClass.checkArgumentCount(method, 0);
        }
    }

    private void checkStarrocksJarUdafClass(CreateFunctionStmt stmt, JavaUDFInternalClass mainClass,
                                            JavaUDFInternalClass udafStateClass) {
        FunctionArgsDef argsDef = stmt.getArgsDef();
        TypeDef returnType = stmt.getReturnType();
        Map<String, String> properties = stmt.getProperties();
        boolean isAnalyticFn = "true".equalsIgnoreCase(properties.get(CreateFunctionStmt.IS_ANALYTIC_NAME));

        {
            // State create()
            Method method = mainClass.getMethod(CreateFunctionStmt.CREATE_METHOD_NAME, true);
            mainClass.checkMethodNonStaticAndPublic(method);
            mainClass.checkReturnJavaType(method, udafStateClass.clazz);
            mainClass.checkArgumentCount(method, 0);
        }
        {
            // void destroy(State);
            Method method = mainClass.getMethod(CreateFunctionStmt.DESTROY_METHOD_NAME, true);
            mainClass.checkMethodNonStaticAndPublic(method);
            mainClass.checkReturnJavaType(method, void.class);
            mainClass.checkArgumentCount(method, 1);
            mainClass.checkParamJavaType(method, udafStateClass.clazz, method.getParameters()[0]);
        }
        {
            // void update(State, ....)
            Method method = mainClass.getMethod(CreateFunctionStmt.UPDATE_METHOD_NAME, true);
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
            Method method = mainClass.getMethod(CreateFunctionStmt.SERIALIZE_METHOD_NAME, true);
            mainClass.checkMethodNonStaticAndPublic(method);
            mainClass.checkReturnJavaType(method, void.class);
            mainClass.checkArgumentCount(method, 2);
            mainClass.checkParamJavaType(method, udafStateClass.clazz, method.getParameters()[0]);
            mainClass.checkParamJavaType(method, java.nio.ByteBuffer.class, method.getParameters()[1]);
        }
        {
            // void merge(State, java.nio.ByteBuffer)
            Method method = mainClass.getMethod(CreateFunctionStmt.MERGE_METHOD_NAME, true);
            mainClass.checkMethodNonStaticAndPublic(method);
            mainClass.checkReturnJavaType(method, void.class);
            mainClass.checkArgumentCount(method, 2);
            mainClass.checkParamJavaType(method, udafStateClass.clazz, method.getParameters()[0]);
            mainClass.checkParamJavaType(method, java.nio.ByteBuffer.class, method.getParameters()[1]);
        }
        {
            // RETURN_TYPE finalize(State);
            Method method = mainClass.getMethod(CreateFunctionStmt.FINALIZE_METHOD_NAME, true);
            mainClass.checkMethodNonStaticAndPublic(method);
            mainClass.checkReturnUdfType(method, returnType.getType());
            mainClass.checkArgumentCount(method, 1);
            mainClass.checkParamJavaType(method, udafStateClass.clazz, method.getParameters()[0]);
        }

        if (isAnalyticFn) {
            {
                Method method = mainClass.getMethod(CreateFunctionStmt.WINDOW_UPDATE_METHOD_NAME, true);
                mainClass.checkMethodNonStaticAndPublic(method);
            }
        }
    }

    private Function analyzeStarrocksJarUdaf(CreateFunctionStmt stmt, String checksum,
                                                JavaUDFInternalClass mainClass,
                                                JavaUDFInternalClass udafStateClass) {
        FunctionName functionName = stmt.getFunctionName();
        FunctionArgsDef argsDef = stmt.getArgsDef();
        TypeDef returnType = stmt.getReturnType();
        String objectFile = stmt.getProperties().get(CreateFunctionStmt.FILE_KEY);
        TypeDef intermediateType = TypeDef.createVarchar(ScalarType.getOlapMaxVarcharLength());
        ;
        Map<String, String> properties = stmt.getProperties();
        boolean isAnalyticFn = "true".equalsIgnoreCase(properties.get(CreateFunctionStmt.IS_ANALYTIC_NAME));

        checkStarrocksJarUdafStateClass(stmt, mainClass, udafStateClass);
        checkStarrocksJarUdafClass(stmt, mainClass, udafStateClass);
        AggregateFunction.AggregateFunctionBuilder builder =
                AggregateFunction.AggregateFunctionBuilder.createUdfBuilder(TFunctionBinaryType.SRJAR);
        builder.name(functionName).argsType(argsDef.getArgTypes()).retType(returnType.getType()).
                hasVarArgs(argsDef.isVariadic()).intermediateType(intermediateType.getType()).objectFile(objectFile)
                .isAnalyticFn(isAnalyticFn)
                .symbolName(mainClass.getCanonicalName());
        Function function = builder.build();
        function.setChecksum(checksum);
        return function;
    }

    private Function analyzeStarrocksJarUdtf(CreateFunctionStmt stmt, String checksum,
                                                JavaUDFInternalClass mainClass) {
        FunctionName functionName = stmt.getFunctionName();
        FunctionArgsDef argsDef = stmt.getArgsDef();
        TypeDef returnType = stmt.getReturnType();
        String objectFile = stmt.getProperties().get(CreateFunctionStmt.FILE_KEY);
        {
            // TYPE[] process(INPUT)
            Method method = mainClass.getMethod(CreateFunctionStmt.PROCESS_METHOD_NAME, true);
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
        return tableFunction;
    }

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
    private static final Class<?> JAVA_ARRAY_CLASS_TYPE = List.class;
    private static final Class<?> JAVA_MAP_CLASS_TYPE = Map.class;

    public static class UDFInternalClassLoader extends URLClassLoader {
        public UDFInternalClassLoader(String udfPath) throws IOException {
            super(new URL[] {new URL("jar:" + udfPath + "!/")});
        }
    }

    private static class UDFSecurityManager extends SecurityManager {
        private Class<?> clazz;

        public UDFSecurityManager(Class<?> clazz) {
            this.clazz = clazz;
        }

        @Override
        public void checkPermission(Permission perm) {
            if (isCreateFromUDFClassLoader()) {
                super.checkPermission(perm);
            }
        }

        public void checkPermission(Permission perm, Object context) {
            if (isCreateFromUDFClassLoader()) {
                super.checkPermission(perm, context);
            }
        }

        private boolean isCreateFromUDFClassLoader() {
            Class<?>[] classContext = getClassContext();
            if (classContext.length >= 2) {
                for (int i = 1; i < classContext.length; i++) {
                    if (classContext[i].getClassLoader() != null &&
                            clazz.equals(classContext[i].getClassLoader().getClass())) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    public static class JavaUDFInternalClass {
        public Class<?> clazz = null;
        public Map<String, Method> methods = null;

        public String getCanonicalName() {
            return clazz.getCanonicalName();
        }

        public void setClazz(Class<?> clazz) {
            this.clazz = clazz;
        }

        public void collectMethods() {
            methods = new HashMap<>();
            for (Method m : clazz.getMethods()) {
                if (!m.getDeclaringClass().equals(clazz)) {
                    continue;
                }
                String name = m.getName();
                if (methods.containsKey(name)) {
                    String errMsg = String.format(
                            "UDF class '%s' has more than one method '%s' ", clazz.getCanonicalName(), name);
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, errMsg);
                }
                methods.put(name, m);
            }
        }

        public Method getMethod(String name, boolean required) {
            Method m = methods.get(name);
            if (m == null && required) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        String.format("UDF class '%s must have method '%s'", clazz.getCanonicalName(), name));
            }
            return m;
        }

        public void checkMethodNonStaticAndPublic(Method method) {
            if (Modifier.isStatic(method.getModifiers())) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        String.format("UDF class '%s' method '%s' should be non-static", clazz.getCanonicalName(),
                                method.getName()));
            }
            if (!Modifier.isPublic(method.getModifiers())) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        String.format("UDF class '%s' method '%s' should be public", clazz.getCanonicalName(),
                                method.getName()));
            }
        }

        private void checkParamJavaType(Method method, Class<?> expType, Parameter p) {
            checkJavaType(method, expType, p.getType(), p.getName());
        }

        private void checkReturnJavaType(Method method, Class<?> expType) {
            checkJavaType(method, expType, method.getReturnType(), CreateFunctionStmt.RETURN_FIELD_NAME);
        }

        private void checkJavaType(Method method, Class<?> expType, Class<?> ptype, String pname) {
            if (!expType.equals(ptype)) {
                String errMsg = String.format("UDF class '%s' method '%s' parameter %s[%s] expect type %s",
                        clazz.getCanonicalName(), method.getName(), pname, ptype.getCanonicalName(),
                        expType.getCanonicalName());
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, errMsg);
            }
        }

        private void checkArgumentCount(Method method, int argumentCount) {
            if (method.getParameters().length != argumentCount) {
                String errMsg = String.format("UDF class '%s' method '%s' expect argument count %d",
                        clazz.getCanonicalName(), method.getName(), argumentCount);
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, errMsg);
            }
        }

        private void checkParamUdfType(Method method, Type expType, Parameter p) {
            checkUdfType(method, expType, p.getType(), p.getName());
        }

        private void checkReturnUdfType(Method method, Type expType) {
            checkUdfType(method, expType, method.getReturnType(), CreateFunctionStmt.RETURN_FIELD_NAME);
        }

        private void checkUdfType(Method method, Type expType, Class<?> ptype, String pname) {
            if (!(expType instanceof ScalarType)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        String.format("UDF class '%s' method '%s' does not support non-scalar type '%s'",
                                clazz.getCanonicalName(), method.getName(), expType));
            }
            ScalarType scalarType = (ScalarType) expType;
            Class<?> cls = PRIMITIVE_TYPE_TO_JAVA_CLASS_TYPE.get(scalarType.getPrimitiveType());
            if (cls == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        String.format("UDF class '%s' method '%s' does not support type '%s'",
                                clazz.getCanonicalName(), method.getName(), scalarType));
            }
            if (!cls.equals(ptype)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        String.format("UDF class '%s' method '%s' parameter %s[%s] type does not match %s",
                                clazz.getCanonicalName(), method.getName(), pname, ptype.getCanonicalName(),
                                cls.getCanonicalName()));
            }
        }

        private void checkScalarReturnUdfType(Method method, Type expType) {
            checkScalarUdfType(method, expType, method.getReturnType(), CreateFunctionStmt.RETURN_FIELD_NAME);
        }

        private void checkScalarUdfType(Method method, Type expType, Class<?> ptype, String pname) {
            if (!(expType instanceof ScalarType)) {
                if (expType.isArrayType()) {
                    if (!ptype.equals(JAVA_ARRAY_CLASS_TYPE)) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                                String.format("UDF class '%s' method '%s' parameter %s[%s] type does not match %s",
                                        clazz.getCanonicalName(), method.getName(), pname, ptype.getCanonicalName(),
                                        JAVA_ARRAY_CLASS_TYPE.getCanonicalName()));
                    }
                    ArrayType arrayType = (ArrayType) expType;
                    if (PRIMITIVE_TYPE_TO_JAVA_CLASS_TYPE.containsKey(arrayType.getItemType().getPrimitiveType())) {
                        return;
                    }
                }

                if (expType.isMapType()) {
                    MapType mapType = (MapType) expType;
                    if (!ptype.equals(JAVA_MAP_CLASS_TYPE)) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                                String.format("UDF class '%s' method '%s' parameter %s[%s] type does not match %s",
                                        clazz.getCanonicalName(), method.getName(), pname, ptype.getCanonicalName(),
                                        JAVA_ARRAY_CLASS_TYPE.getCanonicalName()));
                    }
                    if (PRIMITIVE_TYPE_TO_JAVA_CLASS_TYPE.containsKey(mapType.getKeyType().getPrimitiveType())
                            && PRIMITIVE_TYPE_TO_JAVA_CLASS_TYPE.containsKey(mapType.getValueType().getPrimitiveType())) {
                        return;
                    }
                }
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        String.format("UDF class '%s' method '%s' does not support non-scalar type '%s'",
                                clazz.getCanonicalName(), method.getName(), expType));
            }
            ScalarType scalarType = (ScalarType) expType;
            Class<?> cls = PRIMITIVE_TYPE_TO_JAVA_CLASS_TYPE.get(scalarType.getPrimitiveType());
            if (cls == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        String.format("UDF class '%s' method '%s' does not support type '%s'",
                                clazz.getCanonicalName(), method.getName(), scalarType));
            }
            if (!cls.equals(ptype)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        String.format("UDF class '%s' method '%s' parameter %s[%s] type does not match %s",
                                clazz.getCanonicalName(), method.getName(), pname, ptype.getCanonicalName(),
                                cls.getCanonicalName()));
            }
        }
    }

    private void analyzePython(CreateFunctionStmt stmt) {
        String content = stmt.getContent();
        Map<String, String> properties = stmt.getProperties();
        boolean isInline = content != null;

        String checksum = "";
        if (!isInline) {
            checksum = computeMd5(stmt);
        }
        String symbol = properties.get(CreateFunctionStmt.SYMBOL_KEY);
        String inputType = properties.getOrDefault(CreateFunctionStmt.INPUT_TYPE, "scalar");
        String objectFile = stmt.getProperties().get(CreateFunctionStmt.FILE_KEY);

        if (isInline && !StringUtils.equals(objectFile, "inline")) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_OBJECT, "inline function file should be 'inline'");
        }

        if (!inputType.equalsIgnoreCase("arrow") && !inputType.equalsIgnoreCase("scalar")) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_OBJECT, "unknown input type:", inputType);
        }

        FunctionName functionName = stmt.getFunctionName();
        FunctionArgsDef argsDef = stmt.getArgsDef();
        TypeDef returnType = stmt.getReturnType();
        String isolation = stmt.getProperties().get(CreateFunctionStmt.ISOLATION_KEY);

        ScalarFunction.ScalarFunctionBuilder scalarFunctionBuilder =
                ScalarFunction.ScalarFunctionBuilder.createUdfBuilder(TFunctionBinaryType.PYTHON);
        scalarFunctionBuilder.name(functionName).
                argsType(argsDef.getArgTypes()).
                retType(returnType.getType()).
                hasVarArgs(argsDef.isVariadic()).
                objectFile(objectFile).
                inputType(inputType).
                symbolName(symbol).
                isolation(!"shared".equalsIgnoreCase(isolation)).
                content(content);
        ScalarFunction function = scalarFunctionBuilder.build();
        function.setChecksum(checksum);
        stmt.setFunction(function);
    }
}
