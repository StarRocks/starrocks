// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
import com.starrocks.common.AnalysisException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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

        Class<?> clazz = ScalarOperatorFunctions.class;
        for (Method method : clazz.getDeclaredMethods()) {
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
            mapBuilder.put(signature, new FunctionInvoker(method, signature, annotation.isMetaFunction()));
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

    /**
     * evaluation a fe built-in function
     *
     * @param root CallOperator root
     * @return ConstantOperator if the CallOperator is effect (All child constant/FE builtin function support/....)
     */
    public ScalarOperator evaluation(CallOperator root) {
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

        if (!functions.containsKey(signature)) {
            return root;
        }

        FunctionInvoker invoker = functions.get(signature);

        try {
            ConstantOperator operator = invoker.invoke(root.getChildren());

            // check return result type, decimal will change return type
            if (operator.getType().getPrimitiveType() != fn.getReturnType().getPrimitiveType()) {
                Preconditions.checkState(operator.getType().isDecimalOfAnyVersion());
                Preconditions.checkState(fn.getReturnType().isDecimalOfAnyVersion());
                operator.setType(fn.getReturnType());
            }

            return operator;
        } catch (AnalysisException e) {
            LOG.debug("failed to invoke", e);
            if (invoker.isMetaFunction) {
                throw new StarRocksPlannerException(ErrorType.USER_ERROR, e.getMessage());
            }
        }
        return root;
    }

    private static class FunctionInvoker {
        private final boolean isMetaFunction;
        private final Method method;
        private final FunctionSignature signature;

        public FunctionInvoker(Method method, FunctionSignature signature) {
            this.method = method;
            this.signature = signature;
            this.isMetaFunction = false;
        }

        public FunctionInvoker(Method method, FunctionSignature signature, boolean isMetaFunction) {
            this.method = method;
            this.signature = signature;
            this.isMetaFunction = isMetaFunction;
        }

        public Method getMethod() {
            return method;
        }

        public FunctionSignature getSignature() {
            return signature;
        }

        // Function doesn't support array type
        public ConstantOperator invoke(List<ScalarOperator> args) throws AnalysisException {
            final List<Object> invokeArgs = createInvokeArgs(args);
            try {
                return (ConstantOperator) method.invoke(null, invokeArgs.toArray());
            } catch (InvocationTargetException | IllegalAccessException | IllegalArgumentException e) {
                throw new AnalysisException(e.getLocalizedMessage(), e);
            }
        }

        private List<Object> createInvokeArgs(List<ScalarOperator> args) throws AnalysisException {
            final List<Object> invokeArgs = Lists.newArrayList();
            for (int index = 0; index < method.getParameterTypes().length; index++) {
                final Class<?> argType = method.getParameterTypes()[index];

                if (argType.isArray()) {
                    Preconditions.checkArgument(method.getParameterTypes().length == index + 1);
                    final List<ConstantOperator> variableArgs = Lists.newArrayList();
                    Set<Type> checkSet = Sets.newHashSet();

                    for (int variableArgIndex = index; variableArgIndex < args.size(); variableArgIndex++) {
                        ConstantOperator arg = (ConstantOperator) args.get(variableArgIndex);
                        variableArgs.add(arg);
                        checkSet.add(arg.getType());
                    }

                    // Array data must keep same kinds
                    if (checkSet.size() > 1) {
                        throw new AnalysisException("Function's args does't match.");
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

