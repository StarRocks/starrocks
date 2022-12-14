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

package com.starrocks.udf;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

public class UDFClassAnalyzer {
    static Map<String, String> anlyMap = new HashMap<>();

    static {
        anlyMap.put("boolean", "Z");
        anlyMap.put("byte", "B");
        anlyMap.put("char", "C");
        anlyMap.put("short", "S");
        anlyMap.put("int", "I");
        anlyMap.put("long", "J");
        anlyMap.put("float", "F");
        anlyMap.put("double", "D");
        anlyMap.put("void", "V");
    }

    private static String getSignature(String typeName) {
        String prefix = "";
        if (typeName.contains("[]")) {
            prefix = "[";
            typeName = typeName.replace("[]", "");
        }
        String signStr = anlyMap.get(typeName);
        if (signStr != null) {
            return prefix + signStr;
        } else if (typeName.contains("[L")) {
            return typeName.replace(".", "/");
        } else {
            return prefix + "L" + typeName.replace('.', '/') + ";";
        }
    }

    public static boolean hasMemberMethod(String methodName, Class clazz) {
        for (Method declaredMethod : clazz.getDeclaredMethods()) {
            if (declaredMethod.getName().equals(methodName)) {
                return !Modifier.isStatic(declaredMethod.getModifiers());
            }
        }
        return false;
    }

    public static String getSignature(String methodName, Class clazz) throws NoSuchMethodException {
        for (Method declaredMethod : clazz.getDeclaredMethods()) {
            if (declaredMethod.getName().equals(methodName)) {
                if (Modifier.isStatic(declaredMethod.getModifiers())) {
                    throw new NoSuchMethodException("Found a static method:" + methodName);
                }
                StringBuilder val = new StringBuilder("(");
                for (Type genericParameterType : declaredMethod.getGenericParameterTypes()) {
                    String typeName = genericParameterType.getTypeName();
                    val.append(getSignature(typeName));
                }
                val.append(")");
                val.append(getSignature(declaredMethod.getReturnType().getName()));
                return val.toString();
            }
        }
        throw new NoSuchMethodException("Not Found Method:" + methodName);
    }
    
    public static Method getMethodObject(String methodName, Class clazz) {
        for (Method declaredMethod : clazz.getDeclaredMethods()) {
            if (declaredMethod.getName().equals(methodName)) {
                return declaredMethod;
            }
        }
        return null;
    }
}

