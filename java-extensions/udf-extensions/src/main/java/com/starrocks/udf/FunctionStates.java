// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.udf;

import java.util.ArrayList;
import java.util.List;

public class FunctionStates<T> {
    public List<T> states = new ArrayList<>();

    public T get(int idx) {
        return states.get(idx);
    }

    Object[] batch_get(int[] idxs) {
        Object[] res = new Object[idxs.length];
        for(int i = 0;i < idxs.length; ++i) {
            if (idxs[i] != -1) {
                res[i] = states.get(idxs[i]);
            }
        }
        return res;
    }


    public int add(T state) throws Exception {
        states.add(state);
        return states.size() - 1;
    }

}
