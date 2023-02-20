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

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;

// UDF ClassLoader
// Used to isolate classes between UDFs to avoid the influence between static variables.
// BE UDF's Expr creates a new instance of ClassLoader
// each time it opens. It can ensure no influence on each other.

public class UDFClassLoader extends URLClassLoader {

    private Map<String, Class<?>> genClazzMap = new HashMap<>();
    private static final int SINGLE_BATCH_UPDATE = 1;
    private static final int BATCH_EVALUATE = 2; 

    public UDFClassLoader(String udfPath) throws IOException {
        super(new URL[] {new URL("file://" + udfPath)});
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        String clazzName = name.replace("/", ".");
        if (genClazzMap.containsKey(clazzName)) {
            return genClazzMap.get(clazzName);
        }
        return super.findClass(clazzName);
    }


    public Class<?> generateCallStubV(String name, Class<?> clazz, Method method, int genType) {
        String clazzName = name.replace("/", ".");
        if (!clazzName.startsWith(CallStubGenerator.GEN_KEYWORD)) {
            throw new UnsupportedOperationException(
                    "generate class name should start with " + CallStubGenerator.GEN_KEYWORD);
        }
        byte[] bytes = null;
        if (genType == SINGLE_BATCH_UPDATE) {
            bytes = CallStubGenerator.generateCallStubV(clazz, method);
        } else if (genType == BATCH_EVALUATE) {
            bytes = CallStubGenerator.generateScalarCallStub(clazz, method);
        } else {
            throw new UnsupportedOperationException("Unsupported generate stub type:" + genType);
        }
        final Class<?> genClazz = defineClass(clazzName, bytes, 0, bytes.length);
        genClazzMap.put(name, genClazz);
        return genClazz;
    }
}

