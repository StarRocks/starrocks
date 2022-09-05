// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.udf;

import java.util.ArrayList;
import java.util.List;

public class FunctionStates<T> {
    public List<T> states = new ArrayList<>();

    public T get(int idx) {
        return states.get(idx);
    }

    public int add(T state) throws Exception {
        states.add(state);
        return states.size() - 1;
    }

}
