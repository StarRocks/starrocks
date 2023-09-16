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

package com.starrocks.fs;

import org.apache.hadoop.fs.FileSystem;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;

public class GenProxyMethod {
    static final String[] EXCLUDED_METHODS = {"initialize", "close", "toString"};

    public static String handleMethod(Method m) {
        StringBuilder sb = new StringBuilder();
        sb.append("@Override\n");
        sb.append("public " + m.getReturnType().getTypeName() + " " + m.getName() + "(");
        for (Parameter p : m.getParameters()) {
            sb.append(p.getType().getTypeName().replace("$", ".") + " ");
            sb.append(p.getName());
            sb.append(",");
        }
        if (m.getParameterCount() != 0) {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append(")");
        if (m.getExceptionTypes().length > 0) {
            sb.append(" throws ");
            for (Class ex : m.getExceptionTypes()) {
                sb.append(ex.getTypeName() + ",");
            }
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append(" {\n");
        if (!m.getReturnType().equals(void.class)) {
            sb.append("return ");
        }
        sb.append("fs." + m.getName() + "(");
        for (Parameter p : m.getParameters()) {
            sb.append(p.getName());
            sb.append(",");
        }
        if (m.getParameterCount() != 0) {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append(");\n}\n");
        return sb.toString();
    }

    public static boolean needToOverride(Class cls, Method m) {
        int mod = m.getModifiers();
        if (!Modifier.isPublic(mod)) {
            return false;
        }
        if (Modifier.isStatic(mod) || Modifier.isNative(mod) || Modifier.isFinal(mod)) {
            return false;
        }
        if (!m.getDeclaringClass().equals(cls)) {
            return false;
        }
        for (String ex : EXCLUDED_METHODS) {
            if (ex.equals(m.getName())) {
                return false;
            }
        }
        return true;
    }

    public static void main(String[] args) {
        Class cls = FileSystem.class;
        Method[] methods = cls.getMethods();
        for (Method m : methods) {
            if (needToOverride(cls, m)) {
                String s = handleMethod(m);
                System.out.println(s);
            }
        }
    }
}
