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
import org.apache.hadoop.crypto.CryptoInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.lang.reflect.Method;

public class FileSystemByteBuddyProxy {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(FileSystemByteBuddyProxy.class);

    interface UGIObject {
        UserGroupInformation getUGI();

        Object getTarget();
    }

    abstract static class FSProxy extends FileSystem implements UGIObject {
        private FileSystem target;
        private UserGroupInformation ugi;

        public FSProxy(FileSystem target, UserGroupInformation ugi) {
            this.target = target;
            this.ugi = ugi;
        }

        @Override
        public UserGroupInformation getUGI() {
            return ugi;
        }

        @Override
        public FileSystem getTarget() {
            return target;
        }
    }

    static class FSDataInputStreamProxy extends FSDataInputStream implements UGIObject {
        FSDataInputStream target;
        UserGroupInformation ugi;

        public FSDataInputStreamProxy(FSDataInputStream target, UserGroupInformation ugi) {
            super(target.getWrappedStream());
            this.target = target;
            this.ugi = ugi;
        }

        @Override
        public UserGroupInformation getUGI() {
            return ugi;
        }

        @Override
        public FSDataInputStream getTarget() {
            return target;
        }
    }

    static class HdfsDataInputStreamProxy extends HdfsDataInputStream implements UGIObject {
        HdfsDataInputStream target;
        UserGroupInformation ugi;

        public HdfsDataInputStreamProxy(DFSInputStream in, HdfsDataInputStream target, UserGroupInformation ugi) {
            super(in);
            init(target, ugi);
        }

        public HdfsDataInputStreamProxy(CryptoInputStream in, HdfsDataInputStream target, UserGroupInformation ugi) {
            super(in);
            init(target, ugi);
        }

        private void init(HdfsDataInputStream target, UserGroupInformation ugi) {
            this.target = target;
            this.ugi = ugi;
        }

        @Override
        public UserGroupInformation getUGI() {
            return ugi;
        }

        @Override
        public HdfsDataInputStream getTarget() {
            return target;
        }
    }

    public static Class buildFSProxyClass() {
        Class<FSProxy> cls = FSProxy.class;
        return new ByteBuddy()
                .subclass(cls)
                .method(ElementMatchers.not(ElementMatchers.isDeclaredBy(Object.class))
                        .and(ElementMatchers.not(
                                ElementMatchers.namedOneOf("getTarget", "getUGI"))))
                .intercept(MethodDelegation.to(new GeneralInterceptor(cls.getSimpleName())))
                .make()
                .load(cls.getClassLoader(), ClassLoadingStrategy.Default.INJECTION)
                .getLoaded();
    }

    public static Class buildFSDataInputStreamProxyClass() {
        Class<FSDataInputStreamProxy> cls = FSDataInputStreamProxy.class;
        return new ByteBuddy()
                .subclass(cls)
                .method(ElementMatchers.not(ElementMatchers.isDeclaredBy(Object.class))
                        .and(ElementMatchers.not(ElementMatchers.namedOneOf("getTarget", "getUGI")))
                        .and(ElementMatchers.namedOneOf("open", "read")))
                .intercept(MethodDelegation.to(new GeneralInterceptor(cls.getSimpleName())))
                .make()
                .load(cls.getClassLoader(), ClassLoadingStrategy.Default.INJECTION)
                .getLoaded();
    }

    public static Class buildHdfsDataInputStreamProxyClass() {
        Class<HdfsDataInputStreamProxy> cls = HdfsDataInputStreamProxy.class;
        return new ByteBuddy()
                .subclass(cls)
                .method(ElementMatchers.not(ElementMatchers.isDeclaredBy(Object.class))
                        .and(ElementMatchers.not(ElementMatchers.namedOneOf("getTarget", "getUGI")))
                        .and(ElementMatchers.namedOneOf("open", "read")))
                .intercept(MethodDelegation.to(new GeneralInterceptor(cls.getSimpleName())))
                .make()
                .load(cls.getClassLoader(), ClassLoadingStrategy.Default.INJECTION)
                .getLoaded();
    }

    static class GeneralInterceptor {
        private String name;

        public GeneralInterceptor(String name) {
            this.name = name;
        }

        @RuntimeType
        public Object intercept(@This Object self, @AllArguments Object[] args, @Origin Method method,
                                @SuperMethod(nullIfImpossible = true) Method superMethod)
                throws Exception {
            UGIObject proxy = (UGIObject) self;
            LOGGER.info(HadoopExt.LOGGER_MESSAGE_PREFIX + " intercept: name = " + name + ", method = " + method);

            // During initialization there is no target.
            if (proxy.getTarget() == null) {
                return superMethod.invoke(proxy, args);
            }

            Object res = null;
            // No need to switch current user.
            if (!UserGroupInformation.getCurrentUser().equals(proxy.getUGI())) {
                res = HadoopExt.getInstance().doAs(proxy.getUGI(), () -> method.invoke(proxy.getTarget(), args));
            } else {
                res = method.invoke(proxy.getTarget(), args);
            }

            if (res instanceof HdfsDataInputStream) {
                res = createHdfsDataInputStreamProxy((HdfsDataInputStream) res, proxy.getUGI());
            } else if (res instanceof FSDataInputStream) {
                res = createFSDataInputStreamProxy((FSDataInputStream) res, proxy.getUGI());
            }
            return res;
        }
    }

    static Class FSProxyClass = buildFSProxyClass();
    static Class FSDataInputStreamProxyClass = buildFSDataInputStreamProxyClass();
    static Class HdfsDataInputStreamProxyClass = buildHdfsDataInputStreamProxyClass();

    public static FileSystem createFSProxy(FileSystem target, UserGroupInformation ugi) {
        Object proxy = null;
        try {
            proxy = FSProxyClass.getConstructor(FileSystem.class, UserGroupInformation.class)
                    .newInstance(target, ugi);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        FileSystem fs = (FileSystem) proxy;
        return fs;
    }

    public static FSDataInputStream createFSDataInputStreamProxy(FSDataInputStream target, UserGroupInformation ugi) {
        Object proxy = null;
        try {
            proxy = FSDataInputStreamProxyClass.getConstructor(FSDataInputStream.class, UserGroupInformation.class)
                    .newInstance(target, ugi);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return (FSDataInputStream) proxy;
    }

    public static HdfsDataInputStream createHdfsDataInputStreamProxy(HdfsDataInputStream target, UserGroupInformation ugi) {
        Object proxy = null;
        try {
            InputStream in = target.getWrappedStream();
            if (in instanceof DFSInputStream) {
                proxy = HdfsDataInputStreamProxyClass.getConstructor(DFSInputStream.class, HdfsDataInputStream.class,
                                UserGroupInformation.class)
                        .newInstance(target.getWrappedStream(), target, ugi);
            } else if (in instanceof CryptoInputStream) {
                proxy = HdfsDataInputStreamProxyClass.getConstructor(CryptoInputStream.class, HdfsDataInputStream.class,
                                UserGroupInformation.class)
                        .newInstance(target.getWrappedStream(), target, ugi);
            } else {
                throw new IllegalArgumentException("unexpected input stream");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return (HdfsDataInputStreamProxy) proxy;
    }
}