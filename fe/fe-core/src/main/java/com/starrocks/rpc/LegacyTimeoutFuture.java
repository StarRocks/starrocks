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

package com.starrocks.rpc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Keeps the {@link Future} contract the FE's brpc callers were written against.
 *
 * <p>Up to {@code com.baidu:jprotobuf-rpc-core:4.2.1}, a brpc method declared to return
 * {@link Future} returned the library's own future, whose timed {@code get} behaved differently
 * from {@link java.util.concurrent.CompletableFuture}:
 * <ul>
 *   <li>a timeout of zero or less meant "wait until the response arrives";</li>
 *   <li>running out of time threw {@link ExecutionException} wrapping the {@link TimeoutException},
 *       never a bare {@link TimeoutException}, with a message containing "time out";</li>
 *   <li>{@code cancel} did nothing and returned false.</li>
 * </ul>
 * {@code com.starrocks:jprotobuf-rpc-core} returns a {@code CompletableFuture} for those methods
 * instead, which changes all three. Callers depend on each of them: EXPORT deploys with a zero
 * delivery timeout, the deployer black-lists and retries a backend that does not answer in time
 * because the timeout reaches it as an {@code ExecutionException}, and so on. This wrapper puts the
 * old contract back for every future a brpc service returns.
 *
 * <p>Interrupts are the one difference deliberately not restored: the old future swallowed them and
 * kept spinning, while this one lets {@link InterruptedException} through.
 */
public class LegacyTimeoutFuture<T> implements Future<T> {
    private final Future<T> delegate;

    private LegacyTimeoutFuture(Future<T> delegate) {
        this.delegate = delegate;
    }

    public static <T> Future<T> wrap(Future<T> delegate) {
        if (delegate == null || delegate instanceof LegacyTimeoutFuture) {
            return delegate;
        }
        return new LegacyTimeoutFuture<>(delegate);
    }

    /**
     * Returns a view of {@code service} whose methods that return a {@link Future} return it wrapped
     * by {@link #wrap}. Other methods are passed through untouched.
     */
    @SuppressWarnings("unchecked")
    public static <S> S wrapService(S service, Class<S> serviceInterface) {
        InvocationHandler handler = (proxy, method, args) -> {
            if (method.getDeclaringClass() == Object.class) {
                switch (method.getName()) {
                    case "equals":
                        return proxy == args[0];
                    case "hashCode":
                        return System.identityHashCode(proxy);
                    case "toString":
                        return "LegacyTimeoutFuture.service(" + service + ")";
                    default:
                        break;
                }
            }
            Object result;
            try {
                result = method.invoke(service, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
            if (result instanceof Future && Future.class.isAssignableFrom(method.getReturnType())) {
                return wrap((Future<Object>) result);
            }
            return result;
        };
        return (S) Proxy.newProxyInstance(serviceInterface.getClassLoader(), new Class<?>[] {serviceInterface},
                handler);
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        return delegate.get();
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException {
        if (timeout <= 0 || unit == null) {
            return delegate.get();
        }
        try {
            return delegate.get(timeout, unit);
        } catch (TimeoutException e) {
            // Keep the old library's message word for word: ResultReceiver tells a query timeout from an RPC
            // failure by whether the message contains "time out", and black-lists the backend otherwise.
            throw new ExecutionException("Ocurrs time out with specfied time " + timeout + " " + unit, e);
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return delegate.isDone();
    }
}
