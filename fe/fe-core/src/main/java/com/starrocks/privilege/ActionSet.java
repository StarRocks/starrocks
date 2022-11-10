// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import com.google.gson.annotations.SerializedName;

import java.util.Collection;

public class ActionSet {
    @SerializedName(value = "b")
    protected long bitSet = 0;

    public ActionSet(Collection<Action> actions) {
        for (Action action : actions) {
            bitSet |= (1L << action.getId());
        }
    }

    public ActionSet(ActionSet other) {
        this.bitSet = other.bitSet;
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
