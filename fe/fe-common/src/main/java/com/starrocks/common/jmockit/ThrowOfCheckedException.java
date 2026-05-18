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

/**
 * Modify from mockit.internal.reflection.ThrowOfCheckedException JMockit v1.13
 */
public final class ThrowOfCheckedException {
    private static Exception exceptionToThrow;

    ThrowOfCheckedException() throws Exception {
        throw exceptionToThrow;
    }

    public static synchronized void doThrow(Exception checkedException) {
        exceptionToThrow = checkedException;
        ConstructorReflection.newInstanceUsingDefaultConstructor(ThrowOfCheckedException.class);
    }
}
