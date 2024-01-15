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


package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.catalog.PrimitiveType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ConstantFunction {
    String name();

    PrimitiveType[] argTypes();

    PrimitiveType returnType();

    /**
     * These functions are used to inspect metadata of database objects
     */
    boolean isMetaFunction() default false;

    /**
     * When a function is a possible monotonic function, which means for any value within a range,
     * the first and last endpoints yield the new extreme points after the function mapping with
     * other specific arguments. This helps us to determine the result range according the only
     * first and last endpoints of the input of a function.
     * For example, date_trunc() is a monotonic function while abs() is not. date_format(arg1, pattern)
     * is a monotonic function when pattern is special values.
     */
    boolean isMonotonic() default false;

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @interface List {
        ConstantFunction[] list();
    }
}