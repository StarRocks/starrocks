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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.AnyArrayType;
import com.starrocks.catalog.AnyElementType;
import com.starrocks.catalog.AnyMapType;
import com.starrocks.catalog.AnyStructType;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.TableFunction;
import com.starrocks.catalog.Type;
import com.starrocks.sql.common.TypeManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PolymorphicFunctionAnalyzer {
    private static final Logger LOGGER = LogManager.getLogger(PolymorphicFunctionAnalyzer.class);

    private static Type getSuperType(Type t1, Type t2) {
        if (t1.matchesType(t2)) {
            return t1;
        }
        if (t1.isNull()) {
            return t2;
        }
        if (t2.isNull()) {
            return t1;
        }
        if (t1.isScalarType() && t2.isScalarType()) {
            Type commonType = Type.getCommonType(t1, t2);
            return commonType.isValid() ? commonType : null;
        }
        if (t1.isArrayType() && t2.isArrayType()) {
            Type superElementType = getSuperType(((ArrayType) t1).getItemType(), ((ArrayType) t2).getItemType());
            return superElementType != null ? new ArrayType(superElementType) : null;
        }
        if (t1.isMapType() && t2.isMapType()) {
            Type superKeyType = getSuperType(((MapType) t1).getKeyType(), ((MapType) t2).getKeyType());
            Type superValueType = getSuperType(((MapType) t1).getValueType(), ((MapType) t2).getValueType());
            return superKeyType != null && superValueType != null ? new MapType(superKeyType, superValueType) : null;
        }
        return null;
    }

    private static Function newScalarFunction(ScalarFunction fn, List<Type> newArgTypes, Type newRetType) {
        ScalarFunction newFn = new ScalarFunction(fn.getFunctionName(), newArgTypes, newRetType,
                fn.getLocation(), fn.getSymbolName(), fn.getPrepareFnSymbol(),
                fn.getCloseFnSymbol());
        newFn.setFunctionId(fn.getFunctionId());
        newFn.setChecksum(fn.getChecksum());
        newFn.setBinaryType(fn.getBinaryType());
        newFn.setHasVarArgs(fn.hasVarArgs());
        newFn.setId(fn.getId());
        newFn.setUserVisible(fn.isUserVisible());
        return newFn;
    }

    private static Function newAggregateFunction(AggregateFunction fn, List<Type> newArgTypes, Type newRetType) {
        AggregateFunction newFn = new AggregateFunction(fn.getFunctionName(), newArgTypes, newRetType,
                fn.getIntermediateType(), fn.hasVarArgs());
        newFn.setFunctionId(fn.getFunctionId());
        newFn.setChecksum(fn.getChecksum());
        newFn.setBinaryType(fn.getBinaryType());
        newFn.setHasVarArgs(fn.hasVarArgs());
        newFn.setId(fn.getId());
        newFn.setUserVisible(fn.isUserVisible());
        return newFn;
    }

    private static Type[] resolveArgTypes(Function fn, Type[] inputArgTypes) {
        // Use inputArgTypes length, because function may be a variable arguments
        Type[] resolvedTypes = Arrays.copyOf(inputArgTypes, inputArgTypes.length);
        Type[] argsTypes = fn.getArgs();

        int size = Math.max(argsTypes.length, inputArgTypes.length);
        for (int i = 0; i < size; ++i) {
            Type declType = i >= argsTypes.length ? argsTypes[argsTypes.length - 1] : argsTypes[i];
            Type inputType = inputArgTypes[i];

            // If declaration type is not a pseudo type, use it.
            if (!declType.isPseudoType()) {
                resolvedTypes[i] = declType;
                continue;
            }

            // Need to make input be a valid complex type if the input is Type NULL
            if (declType instanceof AnyArrayType) {
                resolvedTypes[i] = inputType.isNull() ? new ArrayType(Type.BOOLEAN) : inputType;
            } else if (declType instanceof AnyMapType) {
                resolvedTypes[i] = inputType.isNull() ? new MapType(Type.BOOLEAN, Type.BOOLEAN) : inputType;
            } else if (declType instanceof AnyStructType) {
                resolvedTypes[i] = inputType.isNull() ? new StructType(Lists.newArrayList(Type.BOOLEAN)) : inputType;
            } else {
                resolvedTypes[i] = inputType;
            }

            resolvedTypes[i] = AnalyzerUtils.replaceNullType2Boolean(resolvedTypes[i]);
        }
        return resolvedTypes;
    }

    private static Function resolveByReplacingInputs(Function fn, Type[] inputArgTypes) {
        Type[] resolvedArgTypes = resolveArgTypes(fn, inputArgTypes);
        if (fn instanceof ScalarFunction) {
            return newScalarFunction((ScalarFunction) fn, Arrays.asList(resolvedArgTypes), fn.getReturnType());
        }
        if (fn instanceof AggregateFunction) {
            return newAggregateFunction((AggregateFunction) fn, Arrays.asList(resolvedArgTypes), fn.getReturnType());
        }
        return null;
    }

    private static class MapKeysDeduce implements java.util.function.Function<Type[], Type> {
        @Override
        public Type apply(Type[] types) {
            MapType mapType = (MapType) types[0];
            return new ArrayType(mapType.getKeyType());
        }
    }

    private static class MapValuesDeduce implements java.util.function.Function<Type[], Type> {
        @Override
        public Type apply(Type[] types) {
            MapType mapType = (MapType) types[0];
            return new ArrayType(mapType.getValueType());
        }
    }

    private static class MapFilterDeduce implements java.util.function.Function<Type[], Type> {
        @Override
        public Type apply(Type[] types) {
            MapType mapType = (MapType) types[0];
            return new MapType(mapType.getKeyType(), mapType.getValueType());
        }
    }

    private static class MapFromArraysDeduce implements java.util.function.Function<Type[], Type> {
        @Override
        public Type apply(Type[] types) {
            ArrayType keyArrayType = (ArrayType) types[0];
            ArrayType valueArrayType = (ArrayType) types[1];
            return new MapType(keyArrayType.getItemType(), valueArrayType.getItemType());
        }
    }

    // map_apply(lambda of function, map) -> return type of lambda
    private static class MapApplyDeduce implements java.util.function.Function<Type[], Type> {
        @Override
        public Type apply(Type[] types) {
            // fake return type, the real return type is from the right part lambda expression of lambda functions.
            return types[1];
        }
    }

    private static class DistinctMapKeysDeduce implements java.util.function.Function<Type[], Type> {
        @Override
        public Type apply(Type[] types) {
            return types[0];
        }
    }

    private static class CommonDeduce implements java.util.function.Function<Type[], Type> {
        @Override
        public Type apply(Type[] types) {
            Type commonType = TypeManager.getCommonSuperType(Arrays.asList(types));
            Arrays.fill(types, commonType);
            return commonType;
        }
    }

    private static class IfDeduce implements java.util.function.Function<Type[], Type> {
        @Override
        public Type apply(Type[] types) {
            Type commonType = TypeManager.getCommonSuperType(types[1], types[2]);
            types[1] = commonType;
            types[2] = commonType;
            return commonType;
        }
    }

    private static class RowDeduce implements java.util.function.Function<Type[], Type> {
        @Override
        public Type apply(Type[] types) {
            return new StructType(Arrays.asList(types));
        }
    }

    private static final ImmutableMap<String, java.util.function.Function<Type[], Type>> DEDUCE_RETURN_TYPE_FUNCTIONS
            = ImmutableMap.<String, java.util.function.Function<Type[], Type>>builder()
            .put("map_keys", new MapKeysDeduce())
            .put("map_values", new MapValuesDeduce())
            .put("map_from_arrays", new MapFromArraysDeduce())
            .put(FunctionSet.ROW, new RowDeduce())
            .put("map_apply", new MapApplyDeduce())
            .put("map_filter", new MapFilterDeduce())
            .put("distinct_map_keys", new DistinctMapKeysDeduce())
            .put("map_concat", new CommonDeduce())
            .put("if", new IfDeduce())
            .put("ifnull", new CommonDeduce())
            .put("nullif", new CommonDeduce())
            .put("coalesce", new CommonDeduce())
            // it's mock, need handle it in expressionAnalyzer
            .put(FunctionSet.NAMED_STRUCT, new RowDeduce())
            .build();

    private static Function resolveByDeducingReturnType(Function fn, Type[] inputArgTypes) {
        java.util.function.Function<Type[], Type> deduce = DEDUCE_RETURN_TYPE_FUNCTIONS.get(fn.functionName());
        if (deduce == null) {
            return null;
        }
        Type[] resolvedArgTypes = resolveArgTypes(fn, inputArgTypes);
        Type newRetType = deduce.apply(resolvedArgTypes);
        if (fn instanceof ScalarFunction) {
            return newScalarFunction((ScalarFunction) fn, Arrays.asList(resolvedArgTypes), newRetType);
        }
        if (fn instanceof AggregateFunction) {
            return newAggregateFunction((AggregateFunction) fn, Arrays.asList(resolvedArgTypes), newRetType);
        }
        return null;
    }

    /**
     * Inspired by <a href="https://github.com/postgres/postgres/blob/master/src/backend/parser/parse_coerce.c#L1934">...</a>
     * <p>
     * Make sure a polymorphic function is legally callable, and deduce actual argument and result types.
     * <p>
     * If any polymorphic pseudotype is used in a function's arguments or return type, we make sure the
     * actual data types are consistent with each other.
     * 1) If return type is ANYELEMENT, and any argument is ANYELEMENT, use the
     * argument's actual type as the function's return type.
     * 2) If return type is ANYARRAY, and any argument is ANYARRAY, use the
     * argument's actual type as the function's return type.
     * 3) Otherwise, if return type is ANYELEMENT or ANYARRAY, and there is
     * at least one ANYELEMENT, ANYARRAY input, deduce the return type from those inputs, or return null
     * if we can't.
     * </p>
     * <p>
     * Like PostgreSQL, two pseudo-types of special interest are ANY_ARRAY and ANY_ELEMENT, which are collectively
     * called polymorphic types. Any function declared using these types is said to be a polymorphic function.
     * A polymorphic function can operate on many different data types, with the specific data type(s) being
     * determined by the data types actually passed to it in a particular call.
     * <p>
     * Polymorphic arguments and results are tied to each other and are resolved to a specific data type when a
     * query calling a polymorphic function is parsed. Each position (either argument or return value) declared
     * as ANY_ELEMENT is allowed to have any specific actual data type, but in any given call they must all be
     * the same actual type. Each position declared as ANY_ARRAY can have any array data type, but similarly they
     * must all be the same type. Furthermore, if there are positions declared ANY_ARRAY and others declared
     * ANY_ELEMENT, the actual array type in the ANY_ARRAY positions must be an array whose elements are the same
     * type appearing in the ANY_ELEMENT positions.
     * <p>
     * Thus, when more than one argument position is declared with a polymorphic type, the net effect is that only
     * certain combinations of actual argument types are allowed. For example, a function declared as
     * equal(ANY_ELEMENT, ANY_ELEMENT) will take any two input values, so long as they are of the same data type.
     * <p>
     * When the return value of a function is declared as a polymorphic type, there must be at least one argument
     * position that is also polymorphic, and the actual data type supplied as the argument determines the actual
     * result type for that call. For example, if there were not already an array subscripting mechanism, one
     * could define a function that implements subscripting as subscript(ANY_ARRAY, INT) returns ANY_ELEMENT. This
     * declaration constrains the actual first argument to be an array type, and allows the parser to infer the
     * correct result type from the actual first argument's type.
     * </p>
     */
    public static Function generatePolymorphicFunction(Function fn, Type[] paramTypes) {
        if (!fn.isPolymorphic()) {
            return fn;
        }
        Type retType = fn.getReturnType();
        Type[] declTypes = fn.getArgs();
        Function resolvedFunction;

        long numPseudoArgs = Arrays.stream(declTypes).filter(Type::isPseudoType).count();
        // resolve single pseudo type parameter, example: int array_length(ANY_ARRAY)
        if (!retType.isPseudoType() && numPseudoArgs == 1) {
            resolvedFunction = resolveByReplacingInputs(fn, paramTypes);
            if (resolvedFunction != null) {
                return resolvedFunction;
            }
        }
        // deduce by special function
        // TODO: refactor resolve arg types, some from L254, others from L262.
        resolvedFunction = resolveByDeducingReturnType(fn, paramTypes);
        if (resolvedFunction != null) {
            return resolvedFunction;
        }

        // common deduce
        ArrayType typeArray;
        Type typeElement;

        List<Type> allRealElementType = Lists.newArrayList();
        int size = fn.hasVarArgs() ? paramTypes.length : declTypes.length;
        for (int i = 0; i < size; i++) {
            Type declType = i >= declTypes.length ? fn.getVarArgsType() : declTypes[i];
            Type realType = paramTypes[i];
            if (declType instanceof AnyArrayType) {
                if (realType.isNull()) {
                    continue;
                }
                Preconditions.checkState(realType.isArrayType());
                allRealElementType.add(((ArrayType) realType).getItemType());
            } else if (declType instanceof AnyElementType) {
                if (realType.isNull()) {
                    continue;
                }
                allRealElementType.add(realType);
            }
        }

        if (!allRealElementType.isEmpty()) {
            Type commonType = allRealElementType.get(0);
            for (Type type : allRealElementType) {
                commonType = TypeManager.getCommonSuperType(commonType, type);
                if (commonType == null) {
                    LOGGER.warn("could not determine polymorphic type because input has non-match types");
                    return null;
                }
            }
            commonType = AnalyzerUtils.replaceNullType2Boolean(commonType);
            typeArray = new ArrayType(commonType);
            typeElement = commonType;
        } else {
            typeElement = Type.BOOLEAN;
            typeArray = new ArrayType(Type.BOOLEAN);
        }

        if (retType instanceof AnyArrayType) {
            retType = typeArray;
        } else if (retType instanceof AnyElementType) {
            retType = typeElement;
        } else {
            Preconditions.checkState(fn instanceof TableFunction || !retType.isPseudoType());
        }

        Type[] realTypes = new Type[declTypes.length];
        for (int i = 0; i < declTypes.length; i++) {
            if (declTypes[i] instanceof AnyArrayType) {
                realTypes[i] = typeArray;
            } else if (declTypes[i] instanceof AnyElementType) {
                realTypes[i] = typeElement;
            } else {
                realTypes[i] = declTypes[i];
            }
        }

        if (fn instanceof ScalarFunction) {
            return newScalarFunction((ScalarFunction) fn, Arrays.asList(realTypes), retType);
        }
        if (fn instanceof AggregateFunction) {
            return newAggregateFunction((AggregateFunction) fn, Arrays.asList(realTypes), retType);
        }
        if (fn instanceof TableFunction) {
            // Because unnest is a variadic function, and the types of multiple parameters may be inconsistent,
            // the current SR variadic function parsing can only support variadic parameters of the same type.
            // The unnest is treated specially here, and the type of the child is directly used as the unnest function type.
            if (fn.functionName().equals("unnest")) {
                List<Type> realTableFnRetTypes = new ArrayList<>();
                for (Type paramType : paramTypes) {
                    if (!paramType.isArrayType()) {
                        return null;
                    }
                    Type t = ((ArrayType) paramType).getItemType();
                    realTableFnRetTypes.add(t);
                }
                return new TableFunction(fn.getFunctionName(), ((TableFunction) fn).getDefaultColumnNames(),
                        Arrays.asList(paramTypes), realTableFnRetTypes);
            }

            TableFunction tableFunction = (TableFunction) fn;
            List<Type> tableFnRetTypes = tableFunction.getTableFnReturnTypes();
            List<Type> realTableFnRetTypes = new ArrayList<>();
            for (Type t : tableFnRetTypes) {
                if (t instanceof AnyArrayType) {
                    realTableFnRetTypes.add(typeArray);
                } else if (t instanceof AnyElementType) {
                    realTableFnRetTypes.add(typeElement);
                } else {
                    assert !retType.isPseudoType();
                    realTableFnRetTypes.add(t);
                }
            }

            return new TableFunction(fn.getFunctionName(), ((TableFunction) fn).getDefaultColumnNames(),
                    Arrays.asList(realTypes), realTableFnRetTypes);
        }
        LOGGER.error("polymorphic function has unknown type: {}", fn);
        return null;
    }
}

