// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/ExpressionFunctions.java

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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.rewrite.FEFunction;
import com.starrocks.rewrite.FEFunctions;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public enum ExpressionFunctions {
    INSTANCE;

    private static final Logger LOG = LogManager.getLogger(ExpressionFunctions.class);
    private ImmutableMultimap<String, FEFunctionInvoker> functions;

    private ExpressionFunctions() {
        registerFunctions();
    }

    public Expr evalExpr(Expr constExpr) {
        // Function's arg are all LiteralExpr.
        for (Expr child : constExpr.getChildren()) {
            if (!(child instanceof LiteralExpr)) {
                return constExpr;
            }
        }

        if (constExpr instanceof ArithmeticExpr
                || constExpr instanceof FunctionCallExpr
                || constExpr instanceof TimestampArithmeticExpr) {
            Function fn = constExpr.getFn();

            Preconditions.checkNotNull(fn, "Expr's fn can't be null.");

            // return NullLiteral directly iff:
            // 1. Not UDF
            // 2. Not in isNotAlwaysNullResultWithNullParamFunctions
            // 3. Has null parameter
            if (!GlobalStateMgr.getCurrentState()
                    .isNotAlwaysNullResultWithNullParamFunction(fn.getFunctionName().getFunction())
                    && !fn.isUdf()) {
                for (Expr e : constExpr.getChildren()) {
                    if (e instanceof NullLiteral) {
                        // types in decimalv3-typed function instances are wild types(both precision and scale are -1)
                        // the wild types should never escaped outside of function instance resolution.
                        Type type = fn.getReturnType();
                        if (type.isDecimalV3()) {
                            return NullLiteral.create(constExpr.getType());
                        } else {
                            return NullLiteral.create(fn.getReturnType());
                        }
                    }
                }
            }

            List<ScalarType> argTypes = new ArrayList<>();
            ScalarType returnType = (ScalarType) fn.getReturnType();
            for (Type type : fn.getArgs()) {
                if (type.isIntegerType()) {
                    // Because branch 2.3 introduced a more refined four arithmetic type deduction in #4691,
                    // but there are some non-Query Statements that also require constant expression calculation.
                    // For example, SetStatement still uses the old Analyzer and the old version of constant calculation
                    // in 2.3, and the old version of the constant calculation does not support low-precision INT,
                    // so the old version of the constant calculation is adapted here.
                    argTypes.add(Type.BIGINT);
                    returnType = Type.BIGINT;
                } else {
                    argTypes.add((ScalarType) type);
                }
            }
            FEFunctionSignature signature = new FEFunctionSignature(fn.functionName(),
                    argTypes.toArray(new ScalarType[argTypes.size()]), returnType);
            FEFunctionInvoker invoker = getFunction(signature);
            if (invoker != null) {
                try {
                    return invoker.invoke(constExpr.getChildrenWithoutCast());
                } catch (AnalysisException e) {
                    LOG.debug("failed to invoke", e);
                    return constExpr;
                }
            }
        }
        return constExpr;
    }

    private FEFunctionInvoker getFunction(FEFunctionSignature signature) {
        Collection<FEFunctionInvoker> functionInvokers = functions.get(signature.getName());
        if (functionInvokers == null) {
            return null;
        }
        for (FEFunctionInvoker invoker : functionInvokers) {
            if (!invoker.getSignature().returnType.equals(signature.getReturnType())) {
                continue;
            }

            ScalarType[] argTypes1 = invoker.getSignature().getArgTypes();
            ScalarType[] argTypes2 = signature.getArgTypes();

            if (!Arrays.equals(argTypes1, argTypes2)) {
                continue;
            }
            return invoker;
        }
        return null;
    }

    private synchronized void registerFunctions() {
        // double checked locking pattern
        // functions only need to init once
        if (functions != null) {
            return;
        }
        ImmutableMultimap.Builder<String, FEFunctionInvoker> mapBuilder =
                new ImmutableMultimap.Builder<String, FEFunctionInvoker>();
        Class clazz = FEFunctions.class;
        for (Method method : clazz.getDeclaredMethods()) {
            FEFunction annotation = method.getAnnotation(FEFunction.class);
            registerFEFunction(mapBuilder, method, annotation);

            FEFunction.List listAnnotation = method.getAnnotation(FEFunction.List.class);

            if (listAnnotation != null) {
                for (FEFunction f : listAnnotation.list()) {
                    registerFEFunction(mapBuilder, method, f);
                }
            }
        }
        this.functions = mapBuilder.build();
    }

    private void registerFEFunction(ImmutableMultimap.Builder<String, FEFunctionInvoker> mapBuilder,
                                    Method method, FEFunction annotation) {
        if (annotation != null) {
            String name = annotation.name();
            ScalarType returnType = ScalarType.createType(annotation.returnType());
            List<ScalarType> argTypes = new ArrayList<>();
            for (String type : annotation.argTypes()) {
                argTypes.add(ScalarType.createType(type));
            }
            FEFunctionSignature signature = new FEFunctionSignature(name,
                    argTypes.toArray(new ScalarType[argTypes.size()]), returnType);
            mapBuilder.put(name, new FEFunctionInvoker(method, signature));
        }
    }

    public static class FEFunctionInvoker {
        private final Method method;
        private final FEFunctionSignature signature;

        public FEFunctionInvoker(Method method, FEFunctionSignature signature) {
            this.method = method;
            this.signature = signature;
        }

        public Method getMethod() {
            return method;
        }

        public FEFunctionSignature getSignature() {
            return signature;
        }

        // Now ExpressionFunctions does't support function that it's args contain
        // array type except last one.
        public LiteralExpr invoke(List<Expr> args) throws AnalysisException {
            final List<Object> invokeArgs = createInvokeArgs(args);
            try {
                return (LiteralExpr) method.invoke(null, invokeArgs.toArray());
            } catch (InvocationTargetException | IllegalAccessException | IllegalArgumentException e) {
                throw new AnalysisException(e.getLocalizedMessage());
            }
        }

        private List<Object> createInvokeArgs(List<Expr> args) throws AnalysisException {
            final List<Object> invokeArgs = Lists.newArrayList();
            for (int typeIndex = 0; typeIndex < method.getParameterTypes().length; typeIndex++) {
                final Class<?> argType = method.getParameterTypes()[typeIndex];
                if (argType.isArray()) {
                    Preconditions.checkArgument(method.getParameterTypes().length == typeIndex + 1);
                    final List<Expr> variableLengthExprs = Lists.newArrayList();
                    for (int variableLengthArgIndex = typeIndex; variableLengthArgIndex < args.size();
                            variableLengthArgIndex++) {
                        variableLengthExprs.add(args.get(variableLengthArgIndex));
                    }
                    LiteralExpr[] variableLengthArgs = createVariableLengthArgs(variableLengthExprs, typeIndex);
                    invokeArgs.add(variableLengthArgs);
                } else {
                    invokeArgs.add(args.get(typeIndex));
                }
            }
            return invokeArgs;
        }

        private LiteralExpr[] createVariableLengthArgs(List<Expr> args, int typeIndex) throws AnalysisException {
            final Set<Class<?>> classSet = Sets.newHashSet();
            for (Expr e : args) {
                classSet.add(e.getClass());
            }
            if (classSet.size() > 1) {
                // Variable-length args' types can't exceed two kinds.
                throw new AnalysisException("Function's args does't match.");
            }

            final ScalarType argType = signature.getArgTypes()[typeIndex];
            LiteralExpr[] exprs;
            if (argType.isStringType()) {
                exprs = new StringLiteral[args.size()];
            } else if (argType.isFixedPointType()) {
                exprs = new IntLiteral[args.size()];
            } else if (argType.isDateType()) {
                exprs = new DateLiteral[args.size()];
            } else if (argType.isDecimalV2()) {
                exprs = new DecimalLiteral[args.size()];
            } else if (argType.isFloatingPointType()) {
                exprs = new FloatLiteral[args.size()];
            } else if (argType.isBoolean()) {
                exprs = new BoolLiteral[args.size()];
            } else {
                throw new IllegalArgumentException("StarRocks does't support type:" + argType);
            }
            args.toArray(exprs);
            return exprs;
        }
    }

    public static class FEFunctionSignature {
        private final String name;
        private final ScalarType[] argTypes;
        private final ScalarType returnType;

        public FEFunctionSignature(String name, ScalarType[] argTypes, ScalarType returnType) {
            this.name = name;
            this.argTypes = argTypes;
            this.returnType = returnType;
        }

        public ScalarType[] getArgTypes() {
            return argTypes;
        }

        public ScalarType getReturnType() {
            return returnType;
        }

        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("FEFunctionSignature. name: ").append(name).append(", return: ").append(returnType);
            sb.append(", args: ").append(Joiner.on(",").join(argTypes));
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FEFunctionSignature signature = (FEFunctionSignature) o;
            return Objects.equals(name, signature.name) && Arrays.equals(argTypes, signature.argTypes)
                    && Objects.equals(returnType, signature.returnType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, argTypes, returnType);
        }
    }
}
