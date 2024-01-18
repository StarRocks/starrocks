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
import org.jetbrains.annotations.NotNull;

public class PrivilegeEntry implements Comparable<PrivilegeEntry> {
    @SerializedName(value = "a")
    protected ActionSet actionSet;
    @SerializedName(value = "o")
    protected PEntryObject object;
    @SerializedName(value = "g")
    protected boolean withGrantOption;

    public PrivilegeEntry(ActionSet actionSet, PEntryObject object, boolean withGrantOption) {
        this.actionSet = actionSet;
        this.object = object;
        this.withGrantOption = withGrantOption;
    }

    public PrivilegeEntry(PrivilegeEntry other) {
        this.actionSet = new ActionSet(other.actionSet);
        if (other.object == null) {
            this.object = null;
        } else {
            this.object = other.object.clone();
        }
        this.withGrantOption = other.withGrantOption;
    }

    public ActionSet getActionSet() {
        return actionSet;
    }

    public PEntryObject getObject() {
        return object;
    }

    public boolean isWithGrantOption() {
        return withGrantOption;
    }

    @Override
    public int compareTo(@NotNull PrivilegeEntry o) {
        //object is null when objectType is SYSTEM
        if (this.object == null && o.object == null) {
            return 0;
        } else if (this.object == null) {
            return -1;
        } else if (o.object == null) {
            return 1;
        }
        return this.object.compareTo(o.object);
    }
}