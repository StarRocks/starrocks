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


package com.starrocks.authz.authorization;

import com.google.gson.annotations.SerializedName;

import java.util.Collection;

public class ActionSet {
    @SerializedName(value = "b")
    protected long bitSet = 0;

    public ActionSet(Collection<PrivilegeType> actions) {
        for (PrivilegeType action : actions) {
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

    public boolean contains(PrivilegeType action) {
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
