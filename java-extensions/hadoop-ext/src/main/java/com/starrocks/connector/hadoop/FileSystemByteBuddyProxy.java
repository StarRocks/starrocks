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
package com.starrocks.connector.hadoop;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperMethod;
import net.bytebuddy.implementation.bind.annotation.This;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

public class FileSystemByteBuddyProxy {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(FileSystemByteBuddyProxy.class);

    abstract static class FSProxy extends FileSystem {
        private FileSystem target;
        private UserGroupInformation ugi;

        public FSProxy(FileSystem target, UserGroupInformation ugi) {
            this.target = target;
            this.ugi = ugi;
        }

        public static Class dynamicType;

        static {
            dynamicType = new ByteBuddy()
                    .subclass(FSProxy.class)
                    .method(ElementMatchers.isDeclaredBy(FileSystem.class))
                    .intercept(MethodDelegation.to(FSProxy.class))
                    .make()
                    .load(FSProxy.class.getClassLoader(), ClassLoadingStrategy.Default.INJECTION)
                    .getLoaded();
        }

        @RuntimeType
        public static Object intercept(@This Object self, @AllArguments Object[] args, @Origin Method method,
                                       @SuperMethod(nullIfImpossible = true) Method superMethod)
                throws Exception {
            FSProxy proxy = (FSProxy) self;
            LOGGER.debug(HadoopExt.LOGGER_MESSAGE_PREFIX + " fs proxy: " + method.toString());
            Object res = HadoopExt.getInstance().doAs(proxy.ugi, () -> method.invoke(proxy.target, args));
            if (res instanceof FSDataInputStream) {
                res = createFSDataInputStreamProxy((FSDataInputStream) res, proxy.ugi);
            }
            return res;
        }
    }

    static class InputStreamProxy extends FSDataInputStream {
        private FSDataInputStream target;
        private UserGroupInformation ugi;

        public static Class dynamicType;

        static {
            dynamicType = new ByteBuddy()
                    .subclass(InputStreamProxy.class)
                    .method(ElementMatchers.not(ElementMatchers.isDeclaredBy(Object.class)))
                    .intercept(MethodDelegation.to(InputStreamProxy.class))
                    .make()
                    .load(InputStreamProxy.class.getClassLoader(), ClassLoadingStrategy.Default.INJECTION)
                    .getLoaded();
        }

        public InputStreamProxy(FSDataInputStream target, UserGroupInformation ugi) {
            super(target.getWrappedStream());
            this.target = target;
            this.ugi = ugi;
        }

        @RuntimeType
        public static Object intercept(@This Object self, @AllArguments Object[] args, @Origin Method method,
                                       @SuperMethod(nullIfImpossible = true) Method superMethod)
                throws Exception {
            LOGGER.debug(HadoopExt.LOGGER_MESSAGE_PREFIX + " input stream proxy: " + method.toString());
            InputStreamProxy proxy = (InputStreamProxy) self;
            return HadoopExt.getInstance().doAs(proxy.ugi, () -> method.invoke(proxy.target, args));
        }
    }

    public static FileSystem createFSProxy(FileSystem target, UserGroupInformation ugi) {
        Object proxy = null;
        try {
            proxy = FSProxy.dynamicType.getConstructor(FileSystem.class, UserGroupInformation.class)
                    .newInstance(target, ugi);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return (FileSystem) proxy;
    }

    public static FSDataInputStream createFSDataInputStreamProxy(FSDataInputStream target, UserGroupInformation ugi) {
        Object proxy = null;
        try {
            proxy = InputStreamProxy.dynamicType.getConstructor(FSDataInputStream.class, UserGroupInformation.class)
                    .newInstance(target, ugi);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return (FSDataInputStream) proxy;
    }
}