// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License").
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

package com.starrocks.sql.common;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.text.MessageFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ErrorMsgProxy {

    public static final ParserErrorMsg PARSER_ERROR_MSG = create(ParserErrorMsg.class);

    @SuppressWarnings("unchecked")
    private static <T> T create(Class<T> clazz) {
        return (T) Proxy.newProxyInstance(clazz.getClassLoader(),
                new Class[] {clazz},
                new InvocationHandler() {
                    // msg cache for zero arguments method
                    final Map<String, String> cache = new ConcurrentHashMap<>();

                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                        if (args == null || args.length == 0) {
                            String key = clazz.getSimpleName() + method.getName();
                            return cache.computeIfAbsent(key, v -> message(method, args));
                        }
                        return message(method, args);
                    }

                    private String message(Method method, Object[] args) {
                        BaseMessage annotation = method.getAnnotation(BaseMessage.class);
                        if (annotation == null) {
                            return method.getName();
                        }
                        String template = annotation.value();
                        MessageFormat format = new MessageFormat(template);
                        return format.format(args);
                    }
                }
        );
    }


    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface BaseMessage {
        String value();
    }
}
