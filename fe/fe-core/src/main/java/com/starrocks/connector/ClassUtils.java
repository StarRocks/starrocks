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

package com.starrocks.connector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ClassUtils {
    private static final HashMap WRAPPER_TO_PRIMITIVE = new HashMap();
    static {
        WRAPPER_TO_PRIMITIVE.put(Boolean.class, Boolean.TYPE);
        WRAPPER_TO_PRIMITIVE.put(Character.class, Character.TYPE);
        WRAPPER_TO_PRIMITIVE.put(Byte.class, Byte.TYPE);
        WRAPPER_TO_PRIMITIVE.put(Short.class, Short.TYPE);
        WRAPPER_TO_PRIMITIVE.put(Integer.class, Integer.TYPE);
        WRAPPER_TO_PRIMITIVE.put(Float.class, Float.TYPE);
        WRAPPER_TO_PRIMITIVE.put(Long.class, Long.TYPE);
        WRAPPER_TO_PRIMITIVE.put(Double.class, Double.TYPE);
        WRAPPER_TO_PRIMITIVE.put(ArrayList.class, List.class);
        WRAPPER_TO_PRIMITIVE.put(String.class, String.class);
    }

    public static Class<?>[] getCompatibleParamClasses(Object[] args) {
        Class<?>[] argTypes = new Class[args.length];
        for (int i = 0; i < args.length; i++) {
            argTypes[i] = toPrimitiveClass(args[i].getClass());
        }
        return argTypes;
    }

    // return wrapped type if its type is primitive.
    public static Class<?> toPrimitiveClass(Class<?> parameterType) {
        if (!parameterType.isPrimitive() || parameterType.getName().equals("java.util.ArrayList")) {
            Class<?> wrapperType = getWrapperType(parameterType);

            assert wrapperType != null;
            return wrapperType;
        } else {
            return parameterType;
        }
    }

    public static Class<?> getWrapperType(Class<?> primitiveType) {
        return (Class) WRAPPER_TO_PRIMITIVE.get(primitiveType);
    }
}
