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

package com.starrocks.authentication;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.UserIdentity;

import java.util.Map;
import java.util.Set;

public abstract class GroupProvider {
    public static final String GROUP_PROVIDER_PROPERTY_TYPE_KEY = "type";

    @SerializedName(value = "n")
    protected String name;
    @SerializedName(value = "m")
    protected Map<String, String> properties;

    public GroupProvider(String name, Map<String, String> properties) {
        this.name = name;
        this.properties = properties;
    }

    public void init() throws DdlException {

    }

    public void destory() {

    }

    public String getName() {
        return name;
    }

    public String getType() {
        return properties.get("type");
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getComment() {
        return "";
    }

    public abstract Set<String> getGroup(UserIdentity userIdentity);

    public abstract void checkProperty() throws SemanticException;
}
