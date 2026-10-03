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
import com.starrocks.common.util.SqlUtils;
import com.starrocks.context.ai.AIProvider;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.MetaNotFoundException;

import java.util.List;
import java.util.Objects;

/** Provider grants follow the persisted UUID, never a reusable provider name. */
public class AIProviderPEntryObject implements PEntryObject {
    @SerializedName("id")
    private final String id;

    public AIProviderPEntryObject(String id) {
        this.id = Objects.requireNonNull(id);
    }

    public static AIProviderPEntryObject generate(List<String> tokens) throws PrivilegeException {
        if (tokens.size() != 1) {
            throw new PrivilegeException("invalid AI provider object tokens, expected one name: " + tokens);
        }
        String name = tokens.get(0);
        // The analyzer only emits this token for an explicit ON ALL AI PROVIDERS clause.
        if ("*".equals(name)) {
            return new AIProviderPEntryObject(PrivilegeBuiltinConstants.ALL_AI_PROVIDERS_ID);
        }
        AIProvider provider = GlobalStateMgr.getCurrentState().getAIProviderMgr().getProvider(name);
        if (provider == null) {
            throw new PrivObjNotFoundException("cannot find AI provider: " + name);
        }
        return new AIProviderPEntryObject(provider.getId());
    }

    public String getId() {
        return id;
    }

    @Override
    public boolean match(Object other) {
        if (!(other instanceof AIProviderPEntryObject)) {
            return false;
        }
        AIProviderPEntryObject granted = (AIProviderPEntryObject) other;
        return granted.isFuzzyMatching() || id.equals(granted.id);
    }

    @Override
    public boolean isFuzzyMatching() {
        return PrivilegeBuiltinConstants.ALL_AI_PROVIDERS_ID.equals(id);
    }

    @Override
    public boolean validate() {
        return isFuzzyMatching() || GlobalStateMgr.getCurrentState().getAIProviderMgr().getProviderById(id) != null;
    }

    @Override
    public int compareTo(PEntryObject other) {
        AIProviderPEntryObject provider = (AIProviderPEntryObject) other;
        if (isFuzzyMatching()) {
            return provider.isFuzzyMatching() ? 0 : -1;
        }
        if (provider.isFuzzyMatching()) {
            return 1;
        }
        return id.compareTo(provider.id);
    }

    @Override
    public PEntryObject clone() {
        return new AIProviderPEntryObject(id);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof AIProviderPEntryObject && Objects.equals(id, ((AIProviderPEntryObject) other).id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        if (isFuzzyMatching()) {
            return "ALL AI PROVIDERS";
        }
        AIProvider provider = GlobalStateMgr.getCurrentState().getAIProviderMgr().getProviderById(id);
        if (provider == null) {
            throw new MetaNotFoundException("cannot find AI provider: " + id);
        }
        return "'" + SqlUtils.escapeSqlString(provider.getName()) + "'";
    }
}
