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

package com.starrocks.common.util;

import java.lang.reflect.InvocationTargetException;

public class ClassUtil {

    public static <T> T initialize(String className, Class<T> expectedClass) throws ClassNotFoundException,
            NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Class<?> cls = classLoader.loadClass(className);
        if (!isSubclass(cls, expectedClass)) {
            throw new RuntimeException(className + "must extend of " + expectedClass.getSimpleName());
        }

        return (T) cls.getDeclaredConstructor().newInstance();
    }

    private static boolean isSubclass(Class<?> subclass, Class<?> superclass) {
        return subclass.getSuperclass().equals(superclass);
    }
}
