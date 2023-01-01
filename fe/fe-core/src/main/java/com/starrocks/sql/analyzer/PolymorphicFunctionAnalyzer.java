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

import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.AnyArrayType;
import com.starrocks.catalog.AnyElementType;
import com.starrocks.catalog.AnyMapType;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.TableFunction;
import com.starrocks.catalog.Type;
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
        if (t1.isFixedPointType() && t2.isFixedPointType()) {
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
     * TODO(zhuming): throws an exception on error, instead of return null.
     */
    public static Function checkPolymorphicFunction(Function fn, Type[] paramTypes) {
        if (!fn.isPolymorphic()) {
            return fn;
        }
        Type retType = fn.getReturnType();
        Type[] declTypes = fn.getArgs();
        Type[] realTypes = Arrays.copyOf(declTypes, declTypes.length);
        ArrayType typeArray = null;
        Type typeElement = null;
        MapType typeMap = null;
        for (int i = 0; i < declTypes.length; i++) {
            Type declType = declTypes[i];
            Type realType = paramTypes[i];
            if (declType instanceof AnyArrayType) {
                if (realType.isNull()) {
                    continue;
                }
                if (typeArray == null) {
                    typeArray = (ArrayType) realType;
                } else if ((typeArray = (ArrayType) getSuperType(typeArray, realType)) == null) {
                    LOGGER.warn("could not determine polymorphic type because input has non-match types");
                    return null;
                }
            } else if (declType instanceof AnyMapType) {
                if (realType.isNull()) {
                    continue;
                }
                if (typeMap == null) {
                    typeMap = (MapType) realType;
                } else {
                    LOGGER.warn("could not determine polymorphic type because input has two map types");
                    return null;
                }
            } else if (declType instanceof AnyElementType) {
                if (realType.isNull()) {
                    continue;
                }
                if (typeElement == null) {
                    typeElement = realType;
                } else if ((typeElement = getSuperType(typeElement, realType)) == null) {
                    LOGGER.warn("could not determine polymorphic type because input has non-match types");
                    return null;
                }
            } else if (declType.matchesType(realType) || Type.canCastTo(realType, declType)) { // non-pseudo types
                continue;
            } else {
                LOGGER.warn("has unhandled pseudo type '{}'", declType);
                return null;
            }
        }

        if (typeArray != null && typeElement != null) {
            typeArray = (ArrayType) getSuperType(typeArray, new ArrayType(typeElement));
            if (typeArray == null) {
                LOGGER.warn("could not determine polymorphic type because has non-match types");
                return null;
            }
            typeElement = typeArray.getItemType();
        } else if (typeArray != null) {
            typeElement = typeArray.getItemType();
        } else if (typeElement != null) {
            typeArray = new ArrayType(typeElement);
        } else {
            typeElement = Type.NULL;
            typeArray = new ArrayType(Type.NULL);
        }

        if (!typeArray.getItemType().matchesType(typeElement)) {
            LOGGER.warn("could not determine polymorphic type because has non-match types");
            return null;
        }

        if (typeMap != null) {
            if (retType instanceof AnyArrayType) {
                if (fn.functionName().equals("map_keys")) {
                    retType = new ArrayType(typeMap.getKeyType());
                } else if (fn.functionName().equals("map_values")) {
                    retType = new ArrayType(typeMap.getValueType());
                } else {
                    LOGGER.warn("not supported map function");
                    return null;
                }
            }
        } else {
            if (retType instanceof AnyArrayType) {
                retType = typeArray;
            } else if (retType instanceof AnyElementType) {
                retType = typeElement;
            } else if (!(fn instanceof TableFunction)) { //TableFunction don't use retType
                assert !retType.isPseudoType();
            }
        }

        for (int i = 0; i < declTypes.length; i++) {
            if (declTypes[i] instanceof AnyArrayType) {
                realTypes[i] = typeArray;
            } else if (declTypes[i] instanceof AnyMapType) {
                realTypes[i] = typeMap;
            } else if (declTypes[i] instanceof AnyElementType) {
                realTypes[i] = typeElement;
            } else {
                realTypes[i] = declTypes[i];
            }
        }

        if (fn instanceof ScalarFunction) {
            ScalarFunction newFn = new ScalarFunction(fn.getFunctionName(), Arrays.asList(realTypes), retType,
                    fn.getLocation(), ((ScalarFunction) fn).getSymbolName(), ((ScalarFunction) fn).getPrepareFnSymbol(),
                    ((ScalarFunction) fn).getCloseFnSymbol());
            newFn.setFunctionId(fn.getFunctionId());
            newFn.setChecksum(fn.getChecksum());
            newFn.setBinaryType(fn.getBinaryType());
            newFn.setHasVarArgs(fn.hasVarArgs());
            newFn.setId(fn.getId());
            newFn.setUserVisible(fn.isUserVisible());
            return newFn;
        }
        if (fn instanceof AggregateFunction) {
            AggregateFunction newFn = new AggregateFunction(fn.getFunctionName(), Arrays.asList(realTypes), retType,
                    ((AggregateFunction) fn).getIntermediateType(), fn.hasVarArgs());
            newFn.setFunctionId(fn.getFunctionId());
            newFn.setChecksum(fn.getChecksum());
            newFn.setBinaryType(fn.getBinaryType());
            newFn.setHasVarArgs(fn.hasVarArgs());
            newFn.setId(fn.getId());
            newFn.setUserVisible(fn.isUserVisible());
            return newFn;
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
                }
            }

            return new TableFunction(fn.getFunctionName(), ((TableFunction) fn).getDefaultColumnNames(),
                    Arrays.asList(realTypes), realTableFnRetTypes);
        }
        LOGGER.error("polymorphic function has unknown type: {}", fn);
        return null;
    }
}
