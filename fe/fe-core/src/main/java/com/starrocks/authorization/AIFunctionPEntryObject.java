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

package com.starrocks.authorization;

import com.google.gson.annotations.SerializedName;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;
import java.util.Objects;

/** A public built-in AI function family, independent of its overload signatures. */
public class AIFunctionPEntryObject implements PEntryObject {
    @SerializedName("n")
    private final String name;

    private AIFunctionPEntryObject(String name) {
        this.name = name;
    }

    public static AIFunctionPEntryObject generate(List<String> tokens) throws PrivilegeException {
        if (tokens.size() != 1) {
            throw new PrivilegeException("invalid AI function object tokens, expected one name: " + tokens);
        }
        String name = tokens.get(0);
        if ("*".equals(name)) {
            throw new PrivilegeException("AI function wildcard is not supported; use USE AI FUNCTIONS ON SYSTEM");
        }
        String family = GlobalStateMgr.getCurrentState().getGrantableAIFunctionFamily(name);
        if (family == null) {
            throw new PrivObjNotFoundException("cannot find public AI function: " + name);
        }
        return new AIFunctionPEntryObject(family);
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean match(Object other) {
        return equals(other);
    }

    @Override
    public boolean isFuzzyMatching() {
        return false;
    }

    @Override
    public boolean validate() {
        return name != null && name.equals(GlobalStateMgr.getCurrentState().getGrantableAIFunctionFamily(name));
    }

    @Override
    public int compareTo(PEntryObject other) {
        return name.compareTo(((AIFunctionPEntryObject) other).name);
    }

    @Override
    public PEntryObject clone() {
        return new AIFunctionPEntryObject(name);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof AIFunctionPEntryObject && Objects.equals(name, ((AIFunctionPEntryObject) other).name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return name;
    }
}
