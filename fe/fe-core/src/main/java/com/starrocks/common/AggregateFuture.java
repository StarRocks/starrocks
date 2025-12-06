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

package com.starrocks.common;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

public class AggregateFuture<T> implements Future<T> {

    private final List<Future<?>> futures;
    private final Function<List<?>, T> function;

    public AggregateFuture(List<Future<?>> futures, Function<List<?>, T> function) {
        this.futures = futures;
        this.function = function;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return futures.stream().allMatch(f -> f.cancel(mayInterruptIfRunning));
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        List<Object> results = new ArrayList<>(futures.size());
        for (Future<?> future : futures) {
            results.add(future.get());
        }
        return function.apply(results);
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        List<Object> results = new ArrayList<>(futures.size());
        for (Future<?> future : futures) {
            results.add(future.get(timeout, unit));
        }
        return function.apply(results);
    }

    @Override
    public boolean isCancelled() {
        return futures.stream().allMatch(f -> f.isCancelled());
    }

    @Override
    public boolean isDone() {
        return futures.stream().allMatch(f -> f.isDone());
    }
}
