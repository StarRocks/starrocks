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


package com.starrocks.sql.optimizer.rewrite;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.FunctionName;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.function.MetaFunctions;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.rewrite.ScalarOperatorFunctions.SUPPORT_JAVA_STYLE_DATETIME_FORMATTER;

/**
 * Use for execute constant functions
 */
public enum ScalarOperatorEvaluator {
    INSTANCE;

    private static final Logger LOG = LogManager.getLogger(ScalarOperatorEvaluator.class);
    private ImmutableMap<FunctionSignature, FunctionInvoker> functions;

    private ScalarOperatorEvaluator() {
        registerFunctions();
    }

    private synchronized void registerFunctions() {
        // double checked locking pattern
        // functions only need to init once
        if (functions != null) {
            return;
        }

        ImmutableMap.Builder<FunctionSignature, FunctionInvoker> mapBuilder = new ImmutableMap.Builder<>();

        Class<?> metaFunctions = MetaFunctions.class;
        Class<?> clazz = ScalarOperatorFunctions.class;
        for (Method method : ListUtils.union(
                Lists.newArrayList(clazz.getDeclaredMethods()),
                Lists.newArrayList(metaFunctions.getDeclaredMethods()))) {
            ConstantFunction annotation = method.getAnnotation(ConstantFunction.class);
            registerFunction(mapBuilder, method, annotation);

            ConstantFunction.List listAnnotation = method.getAnnotation(ConstantFunction.List.class);

            if (listAnnotation != null) {
                for (ConstantFunction f : listAnnotation.list()) {
                    registerFunction(mapBuilder, method, f);
                }
            }
        }
        this.functions = mapBuilder.build();
    }

    private void registerFunction(ImmutableMap.Builder<FunctionSignature, FunctionInvoker> mapBuilder,
                                  Method method, ConstantFunction annotation) {
        if (annotation != null) {
            String name = annotation.name().toUpperCase();
            Type returnType = ScalarType.createType(annotation.returnType());
            List<Type> argTypes = new ArrayList<>();
            for (PrimitiveType type : annotation.argTypes()) {
                argTypes.add(ScalarType.createType(type));
            }

            FunctionSignature signature = new FunctionSignature(name, argTypes, returnType);
            mapBuilder.put(signature, new FunctionInvoker(method, signature, annotation.isMetaFunction(),
                    annotation.isMonotonic()));
        }
    }

    public Function getMetaFunction(FunctionName name, Type[] args) {
        String nameStr = name.getFunction().toUpperCase();
        // NOTE: only support VARCHAR as return type
        FunctionSignature signature = new FunctionSignature(nameStr, Lists.newArrayList(args), Type.VARCHAR);
        FunctionInvoker invoker = functions.get(signature);
        if (invoker == null || !invoker.isMetaFunction) {
            return null;
        }
        return new Function(name, Lists.newArrayList(args), Type.VARCHAR, false);
    }

    public ScalarOperator evaluation(CallOperator root) {
        return evaluation(root, false);
    }

    /**
     * evaluation a fe built-in function
     *
     * @param root CallOperator root
     * @return ConstantOperator if the CallOperator is effect (All child constant/FE builtin function support/....)
     */
    public ScalarOperator evaluation(CallOperator root, boolean needMonotonic) {
        if (ConnectContext.get() != null
                && ConnectContext.get().getSessionVariable().isDisableFunctionFoldConstants()) {
            return root;
        }

        for (ScalarOperator child : root.getChildren()) {
            if (!OperatorType.CONSTANT.equals(child.getOpType())) {
                return root;
            }
        }

        Function fn = root.getFunction();
        if (fn == null) {
            return root;
        }

        // return Null directly iff:
        // 1. Not UDF
        // 2. Not in isNotAlwaysNullResultWithNullParamFunctions
        // 3. Has null parameter
        // 4. Not assert_true
        if (!GlobalStateMgr.getCurrentState()
                .isNotAlwaysNullResultWithNullParamFunction(fn.getFunctionName().getFunction())
                && !fn.isUdf()
                && !FunctionSet.ASSERT_TRUE.equals(fn.getFunctionName().getFunction())) {
            for (ScalarOperator op : root.getChildren()) {
                if (((ConstantOperator) op).isNull()) {
                    // Should return ConstantOperator.createNull(fn.getReturnType()),
                    // but for keep same with old StarRocks
                    // types in decimalv3-typed function instances are wild types(both precision and scale are -1)
                    // the wild types should never escaped outside of function instance resolution.
                    Type type = fn.getReturnType();
                    if (type.isDecimalV3()) {
                        return ConstantOperator.createNull(root.getType());
                    } else {
                        return ConstantOperator.createNull(fn.getReturnType());
                    }
                }
            }
        }

        List<Type> argTypes = new ArrayList<>(Arrays.asList(fn.getArgs()));

        FunctionSignature signature =
                new FunctionSignature(fn.functionName().toUpperCase(), argTypes, fn.getReturnType());

        FunctionInvoker invoker = functions.get(signature);

        if (invoker == null) {
            return root;
        }

        if (needMonotonic && !isMonotonicFunc(invoker, root)) {
            return root;
        }

        try {
            ConstantOperator operator = invoker.invoke(root.getChildren());
            // check return result type, decimal will change return type
            if (operator.getType().getPrimitiveType() != fn.getReturnType().getPrimitiveType()) {
                Preconditions.checkState(operator.getType().isDecimalOfAnyVersion());
                Preconditions.checkState(fn.getReturnType().isDecimalOfAnyVersion());
                operator.setType(fn.getReturnType());
            }
            return operator;
        } catch (Exception e) {
            LOG.debug("failed to invoke", e);
            if (invoker.isMetaFunction) {
                throw new StarRocksPlannerException(ErrorType.USER_ERROR, ExceptionUtils.getRootCauseMessage(e));
            }
        }
        return root;
    }


    private boolean isMonotonicFunc(FunctionInvoker invoker, CallOperator operator) {
        if (!invoker.isMonotonic) {
            return false;
        }

        if (FunctionSet.DATE_FORMAT.equalsIgnoreCase(invoker.getSignature().getName())) {
            String pattern = operator.getChild(1).toString();
            if (pattern.isEmpty()) {
                return true;
            }

            if (SUPPORT_JAVA_STYLE_DATETIME_FORMATTER.contains(pattern.trim())) {
                return true;
            }
            Optional<String> stripedPattern = stripFormatValue(pattern);
            // "YmdHiSf" or "YmdHisf" ensure the format string value reserve the original date order
            // like date_format('2021-01-01', '%Y-%m-%d') is qualified but date_format('2021-01-01', '%m-%Y-%d') is not
            return stripedPattern.map(e -> "YmdHiSf".startsWith(e) || "YmdHisf".startsWith(e)).orElse(false);
        }

        return true;
    }
    private Optional<String> stripFormatValue(String pattern) {
        StringBuilder builder = new StringBuilder();
        boolean unsupportedFormat = false;
        for (char c : pattern.toCharArray()) {
            switch (c) {
                case 'Y':
                case 'm':
                case 'd':
                case 'H':
                case 'i':
                case 'S':
                case 's':
                case 'f':
                    builder.append(c);
                    break;
                case 'a':
                case 'b':
                case 'c':
                case 'D':
                case 'e':
                case 'h':
                case 'I':
                case 'j':
                case 'k':
                case 'l':
                case 'M':
                case 'p':
                case 'r':
                case 'T':
                case 'U':
                case 'u':
                case 'V':
                case 'v':
                case 'W':
                case 'w':
                case 'X':
                case 'x':
                case 'y':
                    unsupportedFormat = true;
                    break;
                default:
                    // do nothing
            }
        }

        return unsupportedFormat ? Optional.empty() : Optional.of(builder.toString());
    }

    private static class FunctionInvoker {
        private final boolean isMetaFunction;

        private final boolean isMonotonic;
        private final Method method;
        private final FunctionSignature signature;

        public FunctionInvoker(Method method, FunctionSignature signature, boolean isMetaFunction, boolean isMonotonic) {
            this.method = method;
            this.signature = signature;
            this.isMetaFunction = isMetaFunction;
            this.isMonotonic = isMonotonic;
        }

        public Method getMethod() {
            return method;
        }

        public FunctionSignature getSignature() {
            return signature;
        }

        // Function doesn't support array type
        public ConstantOperator invoke(List<ScalarOperator> args) throws IllegalAccessException, InvocationTargetException {
            final List<Object> invokeArgs = createInvokeArgs(args);
            return (ConstantOperator) method.invoke(null, invokeArgs.toArray());
        }

        private List<Object> createInvokeArgs(List<ScalarOperator> args) {
            final List<Object> invokeArgs = Lists.newArrayList();
            for (int index = 0; index < method.getParameterTypes().length; index++) {
                final Class<?> argType = method.getParameterTypes()[index];

                if (argType.isArray()) {
                    Preconditions.checkArgument(method.getParameterTypes().length == index + 1);
                    final List<ConstantOperator> variableArgs = Lists.newArrayList();
                    Set<PrimitiveType> checkSet = Sets.newHashSet();

                    for (int variableArgIndex = index; variableArgIndex < args.size(); variableArgIndex++) {
                        ConstantOperator arg = (ConstantOperator) args.get(variableArgIndex);
                        variableArgs.add(arg);
                        checkSet.add(arg.getType().getPrimitiveType());
                    }

                    // Array data must keep same kinds
                    if (checkSet.size() > 1) {
                        throw new IllegalArgumentException("Function's args does't match.");
                    }

                    ConstantOperator[] argsArray = new ConstantOperator[variableArgs.size()];
                    argsArray = variableArgs.toArray(argsArray);
                    invokeArgs.add(argsArray);
                } else {
                    invokeArgs.add(args.get(index));
                }
            }
            return invokeArgs;
        }

    }

    private static class FunctionSignature {
        private final String name;
        private final List<Type> argTypes;
        private final Type returnType;

        public FunctionSignature(String name, List<Type> argTypes, Type returnType) {
            this.name = name;
            this.argTypes = argTypes;
            this.returnType = returnType;
        }

        public List<Type> getArgTypes() {
            return argTypes;
        }

        public Type getReturnType() {
            return returnType;
        }

        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return "FunctionSignature. name: " + name + ", return: " + returnType
                    + ", args: " + Joiner.on(",").join(argTypes);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ScalarOperatorEvaluator.FunctionSignature signature = (ScalarOperatorEvaluator.FunctionSignature) o;

            List<PrimitiveType> primitiveTypes =
                    argTypes.stream().map(Type::getPrimitiveType).collect(Collectors.toList());
            List<PrimitiveType> sigPrimitiveTypes =
                    signature.argTypes.stream().map(Type::getPrimitiveType).collect(Collectors.toList());
            return Objects.equals(name, signature.name) &&
                    primitiveTypes.equals(sigPrimitiveTypes) &&
                    returnType.matchesType(signature.returnType);
        }

        @Override
        public int hashCode() {
            List<PrimitiveType> primitiveTypes =
                    argTypes.stream().map(Type::getPrimitiveType).collect(Collectors.toList());
            return Objects.hash(name, primitiveTypes, returnType.getPrimitiveType());
        }
    }
}

