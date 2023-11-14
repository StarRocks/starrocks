// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @interface List {
        ConstantFunction[] list();
    }
}