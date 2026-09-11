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
import com.starrocks.catalog.AIModel;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.MetaNotFoundException;

import java.util.List;
import java.util.Objects;

/** A native grant binds to a model identity, not a reusable model name. */
public final class AIModelPEntryObject implements PEntryObject {
    @SerializedName("id")
    private final long id;

    private AIModelPEntryObject(long id) {
        this.id = id;
    }

    public static AIModelPEntryObject forId(long id) {
        if (id <= 0) {
            throw new IllegalArgumentException("AI model identity must be positive");
        }
        return new AIModelPEntryObject(id);
    }

    public static AIModelPEntryObject generate(List<String> tokens) throws PrivilegeException {
        if (tokens.size() != 1) {
            throw new PrivilegeException("AI MODEL requires one unqualified object name");
        }
        String name = tokens.get(0);
        if ("*".equals(name)) {
            return new AIModelPEntryObject(PrivilegeBuiltinConstants.ALL_AI_MODELS_ID);
        }
        AIModel model = GlobalStateMgr.getCurrentState().getAIModelMgr().getByName(name);
        if (model == null) {
            throw new PrivObjNotFoundException("cannot find AI model: " + name);
        }
        return forId(model.getId());
    }

    public long getId() {
        return id;
    }

    @Override
    public boolean match(Object object) {
        if (!(object instanceof AIModelPEntryObject other)) {
            return false;
        }
        return other.isFuzzyMatching() || id == other.id;
    }

    @Override
    public boolean isFuzzyMatching() {
        return id == PrivilegeBuiltinConstants.ALL_AI_MODELS_ID;
    }

    @Override
    public boolean validate() {
        return isFuzzyMatching() || GlobalStateMgr.getCurrentState().getAIModelMgr().getById(id) != null;
    }

    @Override
    public int compareTo(PEntryObject object) {
        if (!(object instanceof AIModelPEntryObject other)) {
            throw new ClassCastException("AI model privilege objects can only be compared with each other");
        }
        // Wildcards precede specific grants: allowGrant may stop at a matching specific entry.
        return Long.compare(id, other.id);
    }

    @Override
    public boolean equals(Object object) {
        return object instanceof AIModelPEntryObject other && id == other.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public PEntryObject clone() {
        return new AIModelPEntryObject(id);
    }

    @Override
    public String toString() {
        if (isFuzzyMatching()) {
            return "ALL AI MODELS";
        }
        AIModel model = GlobalStateMgr.getCurrentState().getAIModelMgr().getById(id);
        if (model == null) {
            throw new MetaNotFoundException("Can't find AI model: " + id);
        }
        return "`" + model.getName().replace("`", "``") + "`";
    }
}
