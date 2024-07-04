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

package com.starrocks.sql.automv.util;

import com.google.common.base.Preconditions;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

public abstract class Result<T> {
    private static <T> Result<T> err(Throwable error) {
        return (Result<T>) new Err(error);
    }

    private static <T> Result<T> ok(T value) {
        return new Ok<>(value);
    }

    public static <T> Result<T> wrap(ThrowableSupplier<T> supplier) {
        try {
            return Result.ok(supplier.get());
        } catch (Throwable error) {
            return Result.err(error);
        }
    }

    public static Result<Unit> wrap(ThrowableAction action) {
        return Result.wrap(() -> {
            action.perform();
            return Unit.INSTANCE;
        });
    }

    public <S> Result<S> bind(ThrowableFunction<T, S> func) {
        if (this instanceof Err) {
            return (Result<S>) this;
        } else {
            Ok<T> ok = (Ok<T>) this;
            return Result.wrap(() -> func.apply(ok.getValue()));
        }
    }

    public Result<Unit> bind(ThrowableConsumer<T> consumer) {
        return bind(arg -> {
            consumer.consume(arg);
            return Unit.INSTANCE;
        });
    }

    public Optional<T> unwrap() {
        if (this instanceof Err) {
            return Optional.empty();
        } else {
            return Optional.of(((Ok<T>) this).getValue());
        }
    }

    public T unwrapOrThrowError() throws Throwable {
        if (this instanceof Err) {
            throw ((Err) this).getError();
        } else {
            return ((Ok<T>) this).getValue();
        }
    }

    public Result<T> ifError(Consumer<Throwable> errHandler) {
        if (this instanceof Err) {
            errHandler.accept(((Err) this).getError());
        }
        return this;
    }

    public T mustUnwrap() {
        Preconditions.checkArgument(this instanceof Ok);
        return ((Ok<T>) this).getValue();
    }

    public Optional<Throwable> maybeError() {
        if (this instanceof Err) {
            return Optional.of(((Err) this).getError());
        }
        return Optional.empty();
    }

    @FunctionalInterface
    public interface ThrowableSupplier<T> {
        T get() throws Throwable;
    }

    @FunctionalInterface
    public interface ThrowableAction {
        void perform() throws Throwable;
    }

    @FunctionalInterface
    public interface ThrowableFunction<T, S> {
        S apply(T arg) throws Throwable;
    }

    @FunctionalInterface
    public interface ThrowableConsumer<T> {
        void consume(T arg) throws Throwable;
    }

    public static final class Ok<T> extends Result<T> {
        private final T value;

        private Ok(T value) {
            this.value = Objects.requireNonNull(value);
        }

        public T getValue() {
            return value;
        }
    }

    public static final class Err extends Result<Object> {
        private final Throwable error;

        private Err(Throwable error) {
            this.error = error;
        }

        public Throwable getError() {
            return error;
        }
    }

    public static final class Unit {
        private static final Unit INSTANCE = new Unit();

        private Unit() {
        }
    }
}
