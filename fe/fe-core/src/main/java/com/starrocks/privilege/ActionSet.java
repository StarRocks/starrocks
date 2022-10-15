// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import com.google.gson.annotations.SerializedName;

import java.util.List;

public class ActionSet implements Cloneable {
    @SerializedName(value = "b")
    protected long bitSet = 0;

    public ActionSet(List<Action> actions) {
        for (Action action : actions) {
            bitSet |= (1L << action.getId());
        }
    }

    @Override
    public Object clone() {
        return new ActionSet(bitSet);
    }

    /**
     * private constructor: only construct by ActionSet itself
     */
    private ActionSet(long bitSet) {
        this.bitSet = bitSet;
    }

    public boolean contains(Action action) {
        return (bitSet & (1L << action.getId())) != 0;
    }

    public void add(ActionSet actionSet) {
        bitSet |= actionSet.bitSet;
    }

    public void remove(ActionSet actionSet) {
        bitSet &= ~actionSet.bitSet;
    }

    public ActionSet difference(ActionSet other) {
        return new ActionSet(~bitSet & other.bitSet);
    }

    public boolean contains(ActionSet other) {
        return (~bitSet & other.bitSet) == 0;
    }

    public boolean isEmpty() {
        return bitSet == 0;
    }
}
