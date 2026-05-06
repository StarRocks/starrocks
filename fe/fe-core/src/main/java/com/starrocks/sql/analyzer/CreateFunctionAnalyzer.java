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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.SqlFunction;
import com.starrocks.catalog.TableFunction;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.udf.StorageHandler;
import com.starrocks.common.udf.StorageHandlerFactory;
import com.starrocks.common.util.UDFInternalClassLoader;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.credential.CloudType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.CreateFunctionStmt;
import com.starrocks.sql.ast.FunctionArgsDef;
import com.starrocks.sql.ast.FunctionRef;
import com.starrocks.sql.ast.HdfsURI;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.TypeDef;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.type.ArrayType;
import com.starrocks.type.MapType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.net.URLClassLoader;
import java.security.MessageDigest;
import java.security.Permission;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class CreateFunctionAnalyzer {

    private String objectFile;

    private String md5sum;

    private String isolation;

    private CloudConfiguration cloudConfiguration;

    public void analyze(CreateFunctionStmt stmt, ConnectContext context) {
        if (!Config.enable_udf) {
            throw new SemanticException(
                    "UDF is not enabled in FE, please configure enable_udf=true in fe/conf/fe.conf");
        }
        loadFunctionProperties(stmt);
        analyzeCommon(stmt, context);

        if (stmt.isBuildFunctionMode()) {
            // build function
            analyzeExpression(stmt, context);
        } else if (stmt.isUdfFunctionMode()) {
            String langType = stmt.getLangType();

            if (CreateFunctionStmt.TYPE_STARROCKS_JAR.equalsIgnoreCase(langType)) {
                String checksum = computeMd5(stmt);
                analyzeJavaUDFClass(stmt, checksum, context);
            } else if (CreateFunctionStmt.TYPE_STARROCKS_PYTHON.equalsIgnoreCase(langType)) {
                analyzePython(stmt, context);
            } else {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "unknown lang type");
            }
        }
    }

    private void analyzeExpression(CreateFunctionStmt stmt, ConnectContext context) {
        Expr expr = stmt.getExpr();
        FunctionArgsDef def = stmt.getArgsDef();
        Preconditions.checkState(def.getArgTypes().length == def.getArgNames().size());

        Map<String, Type> argsMap = Maps.newHashMap();
        for (int i = 0; i < def.getArgNames().size(); i++) {
            String name = def.getArgNames().get(i);
            if (argsMap.containsKey(name)) {
                throw new SemanticException("Duplicate argument name %s in function args", name);
            }
            argsMap.put(name, def.getArgTypes()[i]);
        }
        ExpressionAnalyzer.analyzeExpressionResolveSlot(expr, context, slotRef -> {
            if (!argsMap.containsKey(slotRef.getColName())) {
                throw new SemanticException("Cannot find argument %s in function args", slotRef.getColName());
            }
            slotRef.setType(argsMap.get(slotRef.getColName()));
        });

        FunctionName functionName = FunctionRefAnalyzer.resolveFunctionName(
                stmt.getFunctionRef(),
                stmt.getFunctionRef().isGlobalFunction() ? FunctionRefAnalyzer.GLOBAL_UDF_DB
                        : context.getDatabase());
        FunctionArgsDef argsDef = stmt.getArgsDef();

        String viewSql = AstToSQLBuilder.toSQLWithCredential(expr);
        Function function = new SqlFunction(functionName, argsDef.getArgTypes(), expr.getType(),
                argsDef.getArgNames().toArray(new String[0]), viewSql);
        stmt.setFunction(function);
    }

    private void loadFunctionProperties(CreateFunctionStmt stmt) {
        this.objectFile = stmt.getProperties().get(CreateFunctionStmt.FILE_KEY);
        this.md5sum = stmt.getProperties().get(CreateFunctionStmt.MD5_CHECKSUM);
        this.isolation = stmt.getProperties().get(CreateFunctionStmt.ISOLATION_KEY);
        this.cloudConfiguration = setupCredential(stmt.getProperties());
    }

    private void analyzeCommon(CreateFunctionStmt stmt, ConnectContext context) {
        FunctionRef functionRef = stmt.getFunctionRef();
        FunctionArgsDef argsDef = stmt.getArgsDef();
        String defaultDb = functionRef.isGlobalFunction() ? FunctionRefAnalyzer.GLOBAL_UDF_DB : context.getDatabase();
        FunctionRefAnalyzer.analyzeFunctionRef(functionRef, defaultDb);
        FunctionRefAnalyzer.analyzeArgsDef(argsDef);

        TypeDef returnType = stmt.getReturnType();
        if (returnType != null) {
            TypeDefAnalyzer.analyze(returnType);
        }
    }

    private CloudConfiguration setupCredential(Map<String, String> properties) {
        CloudConfiguration cloudConfiguration =
                CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        if (cloudConfiguration.getCloudType() != CloudType.DEFAULT) {
            return cloudConfiguration;
        }
        return null;
    }

    public String computeMd5(CreateFunctionStmt stmt) {
        String checksum = "";
        if (FeConstants.runningUnitTest) {
            // skip checking checksum when running ut
            checksum = "";
            return checksum;
        }

        if (Strings.isNullOrEmpty(objectFile)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "No 'object_file' in properties");
        }
        try (StorageHandler handler = StorageHandlerFactory.create(cloudConfiguration);
                InputStream inputStream = handler.openStream(objectFile)) {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] buf = new byte[4096];
            int bytesRead;
            while ((bytesRead = inputStream.read(buf)) >= 0) {
                digest.update(buf, 0, bytesRead);
            }
            checksum = Hex.encodeHexString(digest.digest());
        } catch (Exception e) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "cannot compute object's checksum " + e.getMessage());
        }

        if (md5sum != null && !md5sum.equalsIgnoreCase(checksum)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                    "library's checksum is not equal with input, checksum=" + checksum);
        }

        return checksum;
    }

    private void analyzeJavaUDFClass(CreateFunctionStmt stmt, String checksum, ConnectContext context) {
        Map<String, String> properties = stmt.getProperties();
        String className = properties.get(CreateFunctionStmt.SYMBOL_KEY);
        if (Strings.isNullOrEmpty(className)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                    "No '" + CreateFunctionStmt.SYMBOL_KEY + "' in properties");
        }

        JavaUDFInternalClass handleClass = new JavaUDFInternalClass();
        JavaUDFInternalClass stateClass = new JavaUDFInternalClass();

        try {
            try (URLClassLoader classLoader = UDFInternalClassLoader.create(objectFile, cloudConfiguration)) {
                System.setSecurityManager(new UDFSecurityManager(UDFInternalClassLoader.class));
                handleClass.setClazz(classLoader.loadClass(className));
                handleClass.collectMethods();

                if (stmt.isAggregate()) {
                    String stateClassName = className + "$" + CreateFunctionStmt.STATE_CLASS_NAME;
                    stateClass.setClazz(classLoader.loadClass(stateClassName));
                    stateClass.collectMethods();
                }

                // STRUCT validation later calls Class.getRecordComponents(), a
                // native method that lazily resolves each component type through
                // the defining classloader. Closing the classloader before that
                // resolution surfaces as NoClassDefFoundError on any
                // transitively-referenced record-component class. Drive the
                // resolution here, while the classloader is still open, so the
                // analyzer below sees a fully-loaded type tree.
                eagerlyResolveRecordTypes(handleClass);
                if (stmt.isAggregate()) {
                    eagerlyResolveRecordTypes(stateClass);
                }
            } catch (IOException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Failed to load object_file: " + stmt.getProperties().get(CreateFunctionStmt.FILE_KEY) +
                        "," + e.getMessage());
            } catch (ClassNotFoundException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Class '" + className + "' not found in object_file :" +
                                stmt.getProperties().get(CreateFunctionStmt.FILE_KEY) + "," + e.getMessage());
            } catch (Exception e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "other exception when load class. exception:" + e.getMessage());
            }
        } finally {
            System.setSecurityManager(null);
        }

        Function createdFunction = null;
        if (stmt.isScalar()) {
            createdFunction = analyzeStarrocksJarUdf(stmt, checksum, handleClass, context);
        } else if (stmt.isAggregate()) {
            createdFunction = analyzeStarrocksJarUdaf(stmt, checksum, handleClass, stateClass, context);
        } else {
            createdFunction = analyzeStarrocksJarUdtf(stmt, checksum, handleClass, context);
        }

        stmt.setFunction(createdFunction);
    }

    /**
     * Walk every method's parameter and return types, force-loading any record
     * classes (and their transitive record-component classes) so that later
     * analysis can call Class.getRecordComponents() / Class.getDeclaredConstructor()
     * after the originating classloader is closed without triggering a
     * NoClassDefFoundError on a lazily-resolved component type.
     *
     * The contract relies on JVM behavior: once a class has been resolved by a
     * classloader, the resulting Class object remains reachable for the lifetime
     * of the JVM regardless of whether the originating classloader is later
     * closed. getRecordComponents()'s native call uses already-resolved Class
     * objects after this priming step.
     */
    private static void eagerlyResolveRecordTypes(JavaUDFInternalClass handleClass) {
        if (handleClass.methods == null) {
            return;
        }
        Set<Class<?>> visited = new HashSet<>();
        for (Method m : handleClass.methods.values()) {
            for (Class<?> p : m.getParameterTypes()) {
                resolveRecursively(p, visited);
            }
            resolveRecursively(m.getReturnType(), visited);
            // Walk the generic parameter / return types as well so that record
            // classes only reachable through a parameterized signature
            // (List<Inner>, Map<K, V>, ...) get loaded while the UDF classloader
            // is still open. Without this priming, the analyzer's later call to
            // ParameterizedType.getActualTypeArguments() surfaces as
            // TypeNotPresentException once the try-with-resources closes the
            // classloader.
            for (java.lang.reflect.Type gt : m.getGenericParameterTypes()) {
                collectFromGenericType(gt, visited);
            }
            collectFromGenericType(m.getGenericReturnType(), visited);
        }
    }

    private static void resolveRecursively(Class<?> cls, Set<Class<?>> visited) {
        if (cls == null || cls.isPrimitive() || !visited.add(cls)) {
            return;
        }
        if (cls.isArray()) {
            resolveRecursively(cls.getComponentType(), visited);
            return;
        }
        if (cls.isRecord()) {
            // Native call resolves every component class as a side effect.
            RecordComponent[] comps = cls.getRecordComponents();
            for (RecordComponent comp : comps) {
                resolveRecursively(comp.getType(), visited);
                collectFromGenericType(comp.getGenericType(), visited);
            }
        }
    }

    private static void collectFromGenericType(java.lang.reflect.Type t, Set<Class<?>> visited) {
        if (t instanceof Class<?>) {
            resolveRecursively((Class<?>) t, visited);
        } else if (t instanceof ParameterizedType) {
            ParameterizedType pt = (ParameterizedType) t;
            if (pt.getRawType() instanceof Class<?>) {
                resolveRecursively((Class<?>) pt.getRawType(), visited);
            }
            for (java.lang.reflect.Type a : pt.getActualTypeArguments()) {
                collectFromGenericType(a, visited);
            }
        }
    }

    private void checkStarrocksJarUdfClass(CreateFunctionStmt stmt, JavaUDFInternalClass mainClass) {
        FunctionArgsDef argsDef = stmt.getArgsDef();
        TypeDef returnType = stmt.getReturnType();
        // RETURN_TYPE evaluate(...)
        Method method = mainClass.getMethod(CreateFunctionStmt.EVAL_METHOD_NAME, true);
        mainClass.checkMethodNonStaticAndPublic(method);
        mainClass.checkArgumentCount(method, argsDef.getArgTypes().length, argsDef.isVariadic());
        mainClass.checkScalarReturnUdfType(method, returnType.getType());

        // Validate parameter types
        if (argsDef.isVariadic()) {
            mainClass.checkVarargsScalarParameters(method, argsDef.getArgTypes());
        } else {
            for (int i = 0; i < method.getParameters().length; i++) {
                Parameter p = method.getParameters()[i];
                mainClass.checkScalarUdfType(method, argsDef.getArgTypes()[i], p.getType(),
                        p.getParameterizedType(), p.getName());
            }
        }
    }

    private Function analyzeStarrocksJarUdf(CreateFunctionStmt stmt, String checksum,
                                            JavaUDFInternalClass handleClass, ConnectContext context) {
        checkStarrocksJarUdfClass(stmt, handleClass);

        FunctionName functionName = FunctionRefAnalyzer.resolveFunctionName(
                stmt.getFunctionRef(),
                stmt.getFunctionRef().isGlobalFunction() ? FunctionRefAnalyzer.GLOBAL_UDF_DB
                        : context.getDatabase());
        FunctionArgsDef argsDef = stmt.getArgsDef();
        TypeDef returnType = stmt.getReturnType();
        String objectFile = stmt.getProperties().get(CreateFunctionStmt.FILE_KEY);
        Function function = ScalarFunction.createUdf(
                functionName, argsDef.getArgTypes(),
                returnType.getType(), argsDef.isVariadic(), TFunctionBinaryType.SRJAR,
                objectFile, handleClass.getCanonicalName(), "", "", !"shared".equalsIgnoreCase(isolation),
                cloudConfiguration);
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
            mainClass.checkArgumentCount(method, argsDef.getArgTypes().length + 1, argsDef.isVariadic());
            mainClass.checkParamJavaType(method, udafStateClass.clazz, method.getParameters()[0]);
            
            // Validate parameter types
            if (argsDef.isVariadic()) {
                mainClass.checkVarargsParameters(method, argsDef.getArgTypes(), 1);
            } else {
                for (int i = 0; i < argsDef.getArgTypes().length; i++) {
                    mainClass.checkParamUdfType(method, argsDef.getArgTypes()[i], method.getParameters()[i + 1]);
                }
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
                                             JavaUDFInternalClass udafStateClass,
                                             ConnectContext context) {
        FunctionName functionName = FunctionRefAnalyzer.resolveFunctionName(
                stmt.getFunctionRef(),
                stmt.getFunctionRef().isGlobalFunction() ? FunctionRefAnalyzer.GLOBAL_UDF_DB
                        : context.getDatabase());
        FunctionArgsDef argsDef = stmt.getArgsDef();
        TypeDef returnType = stmt.getReturnType();
        String objectFile = stmt.getProperties().get(CreateFunctionStmt.FILE_KEY);
        ScalarType intermediateType = TypeFactory.createVarcharType(com.starrocks.type.TypeFactory.getOlapMaxVarcharLength());

        Map<String, String> properties = stmt.getProperties();
        boolean isAnalyticFn = "true".equalsIgnoreCase(properties.get(CreateFunctionStmt.IS_ANALYTIC_NAME));

        checkStarrocksJarUdafStateClass(stmt, mainClass, udafStateClass);
        checkStarrocksJarUdafClass(stmt, mainClass, udafStateClass);
        AggregateFunction.AggregateFunctionBuilder builder =
                AggregateFunction.AggregateFunctionBuilder.createUdfBuilder(TFunctionBinaryType.SRJAR);
        builder.name(functionName).argsType(argsDef.getArgTypes()).retType(returnType.getType()).
                hasVarArgs(argsDef.isVariadic()).intermediateType(intermediateType).objectFile(objectFile)
                .isAnalyticFn(isAnalyticFn)
                .symbolName(mainClass.getCanonicalName())
                .cloudConfiguration(cloudConfiguration)
                .setIsolationType(!"shared".equalsIgnoreCase(isolation));
        Function function = builder.build();
        function.setChecksum(checksum);
        return function;
    }

    private Function analyzeStarrocksJarUdtf(CreateFunctionStmt stmt, String checksum,
                                             JavaUDFInternalClass mainClass,
                                             ConnectContext context) {
        FunctionName functionName = FunctionRefAnalyzer.resolveFunctionName(
                stmt.getFunctionRef(),
                stmt.getFunctionRef().isGlobalFunction() ? FunctionRefAnalyzer.GLOBAL_UDF_DB
                        : context.getDatabase());
        FunctionArgsDef argsDef = stmt.getArgsDef();
        TypeDef returnType = stmt.getReturnType();
        String objectFile = stmt.getProperties().get(CreateFunctionStmt.FILE_KEY);
        {
            // TYPE[] process(INPUT)
            Method method = mainClass.getMethod(CreateFunctionStmt.PROCESS_METHOD_NAME, true);
            mainClass.checkMethodNonStaticAndPublic(method);
            mainClass.checkArgumentCount(method, argsDef.getArgTypes().length, argsDef.isVariadic());
            
            // Validate parameter types
            if (argsDef.isVariadic()) {
                mainClass.checkVarargsParameters(method, argsDef.getArgTypes(), 0);
            } else {
                for (int i = 0; i < method.getParameters().length; i++) {
                    Parameter p = method.getParameters()[i];
                    mainClass.checkUdfType(method, argsDef.getArgTypes()[i], p.getType(), p.getName());
                }
            }
        }
        final List<Type> argList = Arrays.stream(argsDef.getArgTypes()).collect(Collectors.toList());
        TableFunction tableFunction = new TableFunction(functionName,
                Lists.newArrayList(functionName.getFunction()),
                argList, Lists.newArrayList(returnType.getType()), argsDef.isVariadic());
        tableFunction.setBinaryType(TFunctionBinaryType.SRJAR);
        tableFunction.setChecksum(checksum);
        tableFunction.setLocation(new HdfsURI(objectFile));
        tableFunction.setSymbolName(mainClass.getCanonicalName());
        tableFunction.setCloudConfiguration(cloudConfiguration);
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
                    .put(PrimitiveType.DECIMAL32, java.math.BigDecimal.class)
                    .put(PrimitiveType.DECIMAL64, java.math.BigDecimal.class)
                    .put(PrimitiveType.DECIMAL128, java.math.BigDecimal.class)
                    .put(PrimitiveType.DECIMAL256, java.math.BigDecimal.class)
                    .put(PrimitiveType.DATE, java.time.LocalDate.class)
                    .put(PrimitiveType.DATETIME, java.time.LocalDateTime.class)
                    .build();
    private static final Class<?> JAVA_ARRAY_CLASS_TYPE = List.class;
    private static final Class<?> JAVA_MAP_CLASS_TYPE = Map.class;

    // Defensive bound on STRUCT nesting depth in UDF signatures. SQL types are
    // always finite trees (no recursive STRUCT syntax) so the recursion always
    // terminates, but a pathologically deep DDL could still blow the FE stack
    // before hitting any other error. 64 is comfortably above any practical
    // schema and well below the default FE thread stack budget; matches the
    // ballpark Trino's RowType / Spark's spark.sql.maxStructNestedLevels use.
    private static final int MAX_STRUCT_NESTING = 64;

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
            checkArgumentCount(method, argumentCount, false);
        }

        private void checkArgumentCount(Method method, int argumentCount, boolean isVariadic) {
            if (isVariadic) {
                // For varargs, the Java method must use varargs syntax
                if (!method.isVarArgs()) {
                    String errMsg = String.format(
                            "UDF class '%s' method '%s' must use varargs syntax (...) when function is declared with varargs",
                            clazz.getCanonicalName(), method.getName());
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, errMsg);
                }
                // For varargs, method parameter count should match the declared argument count
                // (the last parameter is the varargs array)
                if (method.getParameterCount() != argumentCount) {
                    String errMsg = String.format(
                            "UDF class '%s' method '%s' varargs parameter count %d does not match declared argument count %d",
                            clazz.getCanonicalName(), method.getName(), method.getParameterCount(), argumentCount);
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, errMsg);
                }
            } else {
                // For non-varargs, exact parameter count match
                if (method.getParameters().length != argumentCount) {
                    String errMsg = String.format("UDF class '%s' method '%s' expect argument count %d",
                            clazz.getCanonicalName(), method.getName(), argumentCount);
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, errMsg);
                }
            }
        }

        private void checkParamUdfType(Method method, Type expType, Parameter p) {
            checkUdfType(method, expType, p.getType(), p.getName());
        }

        private void checkReturnUdfType(Method method, Type expType) {
            checkUdfType(method, expType, method.getReturnType(), CreateFunctionStmt.RETURN_FIELD_NAME);
        }

        private void checkUdfType(Method method, Type expType, Class<?> ptype, String pname) {
            Class<?> cls = null;
            if (expType instanceof ScalarType) {
                ScalarType scalarType = (ScalarType) expType;
                cls = PRIMITIVE_TYPE_TO_JAVA_CLASS_TYPE.get(scalarType.getPrimitiveType());
            } else if (expType instanceof MapType) {
                cls = Map.class;
            } else if (expType instanceof ArrayType) {
                cls = List.class;
            } else if (expType instanceof StructType) {
                // v1: STRUCT is supported as a scalar UDF input/return only. UDAF/UDTF
                // paths route through checkUdfType/checkParamUdfType and need additional
                // BE plumbing (convert_to_boxed_array does not have access to evaluate
                // parameter classes for UDAF/UDTF), so reject explicitly.
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        String.format("UDF class '%s' method '%s' parameter %s: STRUCT is currently " +
                                        "supported for scalar UDFs only, not UDAF/UDTF",
                                clazz.getCanonicalName(), method.getName(), pname));
                return;
            } else {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        String.format("UDF class '%s' method '%s' does not support type '%s'",
                                clazz.getCanonicalName(), method.getName(), expType));
            }
            if (cls == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        String.format("UDF class '%s' method '%s' does not support type '%s'",
                                clazz.getCanonicalName(), method.getName(), expType));
            }
            if (!cls.equals(ptype)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        String.format("UDF class '%s' method '%s' parameter %s[%s] type does not match %s",
                                clazz.getCanonicalName(), method.getName(), pname, ptype.getCanonicalName(),
                                cls.getCanonicalName()));
            }
        }

        private void checkScalarReturnUdfType(Method method, Type expType) {
            checkScalarUdfType(method, expType, method.getReturnType(), method.getGenericReturnType(),
                    CreateFunctionStmt.RETURN_FIELD_NAME);
        }

        private void checkScalarUdfType(Method method, Type expType, Class<?> ptype,
                                        java.lang.reflect.Type genericType, String pname) {
            // ARRAY / MAP / STRUCT all need to thread the parameterized Java type so the
            // analyzer can drill into List<Record> / Map<*,Record> / record components and
            // bind nested STRUCT slots to their formal record classes. checkUdfFieldType is
            // the recursive driver; for SCALAR we keep the existing fast path.
            if (expType instanceof ScalarType) {
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
                return;
            }
            if (expType.isArrayType() || expType.isMapType() || expType instanceof StructType) {
                checkUdfFieldType(method, expType, ptype, genericType, pname, 0);
                return;
            }
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                    String.format("UDF class '%s' method '%s' does not support non-scalar type '%s'",
                            clazz.getCanonicalName(), method.getName(), expType));
        }

        /**
         * Validate that a Java record class matches a StarRocks STRUCT type.
         *
         * Component count must match field count, and each component's type must match
         * the corresponding StructField type by position. Field names are not enforced
         * (Java identifiers cannot represent every legal SQL field name); a mismatch is
         * tolerated since the binding is positional.
         *
         * Recurses into nested STRUCT (component must be a record), ARRAY (component
         * must be List with a parameterized element), and MAP (component must be Map
         * with parameterized key/value). `depth` counts struct nesting and is bounded
         * by MAX_STRUCT_NESTING; ARRAY/MAP wrappers do not increment it because they
         * cannot themselves form an unbounded recursion in a finite SQL type tree.
         */
        private void checkStructRecord(Method method, StructType structType, Class<?> ptype, String pname, int depth) {
            if (depth >= MAX_STRUCT_NESTING) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        String.format("UDF class '%s' method '%s' parameter %s: STRUCT nesting exceeds limit %d",
                                clazz.getCanonicalName(), method.getName(), pname, MAX_STRUCT_NESTING));
            }
            if (!ptype.isRecord()) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        String.format("UDF class '%s' method '%s' parameter %s[%s] must be a Java record " +
                                        "to bind STRUCT type '%s'",
                                clazz.getCanonicalName(), method.getName(), pname,
                                ptype.getCanonicalName(), structType));
            }
            RecordComponent[] comps = ptype.getRecordComponents();
            List<StructField> fields = structType.getFields();
            if (comps.length != fields.size()) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        String.format("UDF class '%s' method '%s' parameter %s[%s] has %d record components but " +
                                        "STRUCT type '%s' has %d fields",
                                clazz.getCanonicalName(), method.getName(), pname,
                                ptype.getCanonicalName(), comps.length, structType, fields.size()));
            }
            for (int i = 0; i < comps.length; i++) {
                RecordComponent comp = comps[i];
                StructField field = fields.get(i);
                checkUdfFieldType(method, field.getType(), comp.getType(), comp.getGenericType(),
                        pname + "." + comp.getName(), depth + 1);
            }
        }

        /**
         * Recursive type validator that knows about generic signatures. Unlike
         * checkScalarUdfType, this routes through nested STRUCT/ARRAY/MAP without
         * the top-level only restrictions, so it can be used to validate record
         * components.
         *
         * STRUCT may appear in any position: as a field of another STRUCT, as an
         * ARRAY element, or as a MAP key/value. The Java side preserves the formal
         * record class through RecordComponent.getGenericType() / ParameterizedType
         * actual arguments, so the BE can drill in without a per-cell side channel.
         */
        private void checkUdfFieldType(Method method, Type expType, Class<?> rawType,
                                       java.lang.reflect.Type genericType, String pname, int depth) {
            if (expType instanceof ScalarType) {
                ScalarType scalarType = (ScalarType) expType;
                Class<?> expCls = PRIMITIVE_TYPE_TO_JAVA_CLASS_TYPE.get(scalarType.getPrimitiveType());
                if (expCls == null) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                            String.format("UDF class '%s' method '%s' field %s does not support type '%s'",
                                    clazz.getCanonicalName(), method.getName(), pname, expType));
                }
                if (!expCls.equals(rawType)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                            String.format("UDF class '%s' method '%s' field %s[%s] type does not match %s",
                                    clazz.getCanonicalName(), method.getName(), pname,
                                    rawType.getCanonicalName(), expCls.getCanonicalName()));
                }
                return;
            }
            if (expType instanceof ArrayType) {
                if (!JAVA_ARRAY_CLASS_TYPE.equals(rawType)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                            String.format("UDF class '%s' method '%s' field %s[%s] must be %s for ARRAY type",
                                    clazz.getCanonicalName(), method.getName(), pname,
                                    rawType.getCanonicalName(), JAVA_ARRAY_CLASS_TYPE.getCanonicalName()));
                }
                ArrayType arrayType = (ArrayType) expType;
                // Recursive Java-side validation only kicks in when the element subtree
                // contains a STRUCT — that's the case where we need the parameterized
                // type to recover the formal record class. For non-STRUCT subtrees we
                // honor the pre-refactor leniency: raw `List`, `List<?>`, `List<Object>`
                // are all accepted as long as the SQL element type itself is supported.
                if (typeSubtreeHasStruct(arrayType.getItemType())) {
                    java.lang.reflect.Type[] tArgs = extractTypeArguments(method, genericType, pname, 1);
                    Class<?> elemRaw = rawClass(method, tArgs[0], pname);
                    checkUdfFieldType(method, arrayType.getItemType(), elemRaw, tArgs[0], pname + "[]", depth);
                } else if (!isSupportedScalarUdfType(arrayType.getItemType())) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                            String.format("UDF class '%s' method '%s' field %s does not support type '%s'",
                                    clazz.getCanonicalName(), method.getName(), pname, expType));
                }
                return;
            }
            if (expType instanceof MapType) {
                if (!JAVA_MAP_CLASS_TYPE.equals(rawType)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                            String.format("UDF class '%s' method '%s' field %s[%s] must be %s for MAP type",
                                    clazz.getCanonicalName(), method.getName(), pname,
                                    rawType.getCanonicalName(), JAVA_MAP_CLASS_TYPE.getCanonicalName()));
                }
                MapType mapType = (MapType) expType;
                if (typeSubtreeHasStruct(mapType.getKeyType()) || typeSubtreeHasStruct(mapType.getValueType())) {
                    java.lang.reflect.Type[] tArgs = extractTypeArguments(method, genericType, pname, 2);
                    Class<?> keyRaw = rawClass(method, tArgs[0], pname);
                    Class<?> valRaw = rawClass(method, tArgs[1], pname);
                    checkUdfFieldType(method, mapType.getKeyType(), keyRaw, tArgs[0], pname + ".<key>", depth);
                    checkUdfFieldType(method, mapType.getValueType(), valRaw, tArgs[1], pname + ".<value>", depth);
                } else if (!isSupportedScalarUdfType(mapType.getKeyType())
                        || !isSupportedScalarUdfType(mapType.getValueType())) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                            String.format("UDF class '%s' method '%s' field %s does not support type '%s'",
                                    clazz.getCanonicalName(), method.getName(), pname, expType));
                }
                return;
            }
            if (expType instanceof StructType) {
                // STRUCT in any position: drill into the formal record class. The genericType
                // chain (ParameterizedType actual arguments / RecordComponent generic types)
                // preserves the record class even through ARRAY/MAP wrappers, so the BE can
                // walk a parallel UdfTypeDesc tree to materialize records on both input and
                // output paths.
                checkStructRecord(method, (StructType) expType, rawType, pname, depth);
                return;
            }
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                    String.format("UDF class '%s' method '%s' field %s does not support type '%s'",
                            clazz.getCanonicalName(), method.getName(), pname, expType));
        }

        // True iff the SQL type tree rooted at `t` contains any STRUCT node. Used to
        // decide whether checkUdfFieldType must traverse the parameterized Java type
        // (to recover the formal record class) or can fall back to the pre-refactor
        // SQL-only validation that tolerated raw / wildcarded List / Map.
        private static boolean typeSubtreeHasStruct(Type t) {
            if (t instanceof StructType) {
                return true;
            }
            if (t.isArrayType()) {
                return typeSubtreeHasStruct(((ArrayType) t).getItemType());
            }
            if (t.isMapType()) {
                MapType mt = (MapType) t;
                return typeSubtreeHasStruct(mt.getKeyType()) || typeSubtreeHasStruct(mt.getValueType());
            }
            return false;
        }

        // Pre-refactor SQL-only type support check. Kept for the non-STRUCT branches of
        // checkUdfFieldType so existing UDFs that declare raw List / Map / List<?> for
        // ARRAY<scalar> / MAP<scalar,scalar> continue to validate without needing to
        // upgrade their Java signatures to the parameterized form.
        private static boolean isSupportedScalarUdfType(Type type) {
            if (type instanceof ScalarType) {
                return PRIMITIVE_TYPE_TO_JAVA_CLASS_TYPE.containsKey(type.getPrimitiveType());
            }
            if (type.isArrayType()) {
                return isSupportedScalarUdfType(((ArrayType) type).getItemType());
            }
            if (type.isMapType()) {
                MapType mapType = (MapType) type;
                return isSupportedScalarUdfType(mapType.getKeyType())
                        && isSupportedScalarUdfType(mapType.getValueType());
            }
            return false;
        }

        private java.lang.reflect.Type[] extractTypeArguments(Method method,
                                                               java.lang.reflect.Type genericType,
                                                               String pname, int expectedArity) {
            if (!(genericType instanceof ParameterizedType)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        String.format("UDF class '%s' method '%s' field %s must declare generic " +
                                        "parameters (got raw type '%s')",
                                clazz.getCanonicalName(), method.getName(), pname, genericType));
            }
            java.lang.reflect.Type[] tArgs = ((ParameterizedType) genericType).getActualTypeArguments();
            if (tArgs.length != expectedArity) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        String.format("UDF class '%s' method '%s' field %s expected %d generic " +
                                        "parameters but got %d",
                                clazz.getCanonicalName(), method.getName(), pname, expectedArity, tArgs.length));
            }
            return tArgs;
        }

        private Class<?> rawClass(Method method, java.lang.reflect.Type t, String pname) {
            if (t instanceof Class<?>) {
                return (Class<?>) t;
            }
            if (t instanceof ParameterizedType) {
                java.lang.reflect.Type raw = ((ParameterizedType) t).getRawType();
                if (raw instanceof Class<?>) {
                    return (Class<?>) raw;
                }
            }
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                    String.format("UDF class '%s' method '%s' field %s has unsupported generic " +
                                    "signature '%s' (wildcards and type variables are not supported)",
                            clazz.getCanonicalName(), method.getName(), pname, t));
            return null;
        }

        /**
         * Functional interface for type checking strategies. `genericType` carries the
         * formal Java parameterized type (Parameter.getParameterizedType() or the array
         * component for the varargs slot) so the strategy can drill into nested STRUCT
         * record classes; SCALAR strategies that don't care can ignore it.
         */
        @FunctionalInterface
        private interface TypeChecker {
            void check(Method method, Type expectedType, Class<?> actualType,
                       java.lang.reflect.Type genericType, String paramName);
        }

        /**
         * Generic helper to check varargs parameters with a custom type checking strategy.
         * For varargs, the last declared type is the element type of the varargs array.
         * All fixed parameters are checked normally, and the varargs parameter is validated
         * to be an array of the expected element type.
         */
        private void checkVarargsParametersGeneric(Method method, Type[] declaredArgTypes,
                                                   int paramOffset, TypeChecker typeChecker) {
            Parameter[] params = method.getParameters();

            // All parameters except the last should match the declared types
            for (int i = 0; i < declaredArgTypes.length - 1; i++) {
                Parameter param = params[i + paramOffset];
                typeChecker.check(method, declaredArgTypes[i], param.getType(),
                        param.getParameterizedType(), param.getName());
            }

            // The last parameter should be a varargs array
            if (declaredArgTypes.length > 0) {
                Type varargsElementType = declaredArgTypes[declaredArgTypes.length - 1];
                Parameter varargsParam = params[paramOffset + declaredArgTypes.length - 1];

                // Java varargs are represented as arrays
                if (!varargsParam.getType().isArray()) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                            String.format("UDF class '%s' method '%s' varargs parameter '%s' must be an array",
                                    clazz.getCanonicalName(), method.getName(), varargsParam.getName()));
                }

                // Check that the array component type matches the declared varargs element type.
                // Use getGenericComponentType when the varargs is a parameterized array (e.g.
                // List<Inner>...) so nested STRUCT inside the element preserves its record class.
                Class<?> arrayComponentType = varargsParam.getType().getComponentType();
                java.lang.reflect.Type arrayComponentGeneric = arrayComponentType;
                java.lang.reflect.Type varargsGeneric = varargsParam.getParameterizedType();
                if (varargsGeneric instanceof java.lang.reflect.GenericArrayType) {
                    arrayComponentGeneric = ((java.lang.reflect.GenericArrayType) varargsGeneric)
                            .getGenericComponentType();
                }
                typeChecker.check(method, varargsElementType, arrayComponentType, arrayComponentGeneric,
                        varargsParam.getName());
            }
        }

        /**
         * Check varargs parameters for scalar UDFs.
         * Uses checkScalarUdfType for type validation.
         */
        private void checkVarargsScalarParameters(Method method, Type[] declaredArgTypes) {
            checkVarargsParametersGeneric(method, declaredArgTypes, 0, this::checkScalarUdfType);
        }

        /**
         * Check varargs parameters for UDAFs and UDTFs.
         * Uses checkUdfType for type validation with an offset for the first parameter.
         * UDAF/UDTF only support flat scalar/array/map types, so genericType is unused.
         */
        private void checkVarargsParameters(Method method, Type[] declaredArgTypes, int paramOffset) {
            checkVarargsParametersGeneric(method, declaredArgTypes, paramOffset,
                    (m, expType, ptype, gen, pname) -> checkUdfType(m, expType, ptype, pname));
        }
    }

    private void analyzePython(CreateFunctionStmt stmt, ConnectContext context) {
        String content = stmt.getContent();
        Map<String, String> properties = stmt.getProperties();
        boolean isInline = content != null;

        String checksum = "";
        if (!isInline) {
            checksum = computeMd5(stmt);
        }
        String symbol = properties.get(CreateFunctionStmt.SYMBOL_KEY);
        String inputType = properties.getOrDefault(CreateFunctionStmt.INPUT_TYPE, "scalar");
        String objectFile = properties.getOrDefault(CreateFunctionStmt.FILE_KEY, "inline");

        if (isInline && !StringUtils.equalsIgnoreCase(objectFile, "inline")) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "inline function file should be 'inline'");
        }

        if (!inputType.equalsIgnoreCase("arrow") && !inputType.equalsIgnoreCase("scalar")) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "unknown input type:" + inputType);
        }

        FunctionName functionName = FunctionRefAnalyzer.resolveFunctionName(
                stmt.getFunctionRef(),
                stmt.getFunctionRef().isGlobalFunction() ? FunctionRefAnalyzer.GLOBAL_UDF_DB
                        : context.getDatabase());
        FunctionArgsDef argsDef = stmt.getArgsDef();
        TypeDef returnType = stmt.getReturnType();

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
