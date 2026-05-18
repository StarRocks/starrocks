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
//
// Derived from JMockit (MIT-licensed):
// Copyright (c) 2006 JMockit developers
// https://github.com/jmockit/jmockit2/blob/master/LICENSE.txt

package com.starrocks.common.jmockit;

import java.lang.reflect.Proxy;

/**
 * Modify from mockit.internal.util.GeneratedClasses JMockit v1.13
 * Helper class to return type of mocked-object
 */
public final class GeneratedClasses {
    private static final String IMPLCLASS_PREFIX = "$Impl_";
    private static final String SUBCLASS_PREFIX = "$Subclass_";

    private GeneratedClasses() {
    }

    static boolean isGeneratedImplementationClass(Class<?> mockedType) {
        return isGeneratedImplementationClass(mockedType.getName());
    }

    static boolean isGeneratedImplementationClass(String className) {
        return className.contains(IMPLCLASS_PREFIX);
    }

    static boolean isGeneratedSubclass(String className) {
        return className.contains(SUBCLASS_PREFIX);
    }

    static boolean isGeneratedClass(String className) {
        return isGeneratedSubclass(className) || isGeneratedImplementationClass(className);
    }

    static Class<?> getMockedClassOrInterfaceType(Class<?> aClass) {
        if (!Proxy.isProxyClass(aClass) && !isGeneratedImplementationClass(aClass)) {
            return isGeneratedSubclass(aClass.getName()) ? aClass.getSuperclass() : aClass;
        } else {
            return aClass.getInterfaces()[0];
        }
    }

    static Class<?> getMockedClass(Object mock) {
        return getMockedClassOrInterfaceType(mock.getClass());
    }
}
