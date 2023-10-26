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

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.proc.BaseProcResult;

import java.util.Map;

public class OdpsResource extends Resource {

    public static final String TYPE = "type";
    public static final String NAME = "name";
    public static final String ENDPOINT = "endpoint";
    public static final String DEFAULT_PROJECT = "default_project";
    public static final String ACCESS_ID = "access_id";
    public static final String ACCESS_KEY = "access_key";

    @SerializedName(value = "configs")
    private Map<String, String> configs;

    public OdpsResource(String name) {
        super(name, ResourceType.ODPS);
    }

    public OdpsResource(String name, Map<String, String> configs) {
        super(name, ResourceType.ODPS);
        this.configs = configs;
    }

    @Override
    protected void setProperties(Map<String, String> properties) throws DdlException {
        Preconditions.checkState(properties != null);
        for (String key : properties.keySet()) {
            if (!ENDPOINT.equals(key) && !DEFAULT_PROJECT.equals(key) && !ACCESS_ID.equals(key)
                    && !ACCESS_KEY.equals(key) && !TYPE.equals(key) && !NAME.equals(key)) {
                throw new DdlException("Property " + key + " is unknown");
            }
        }
        this.configs = properties;
        checkProperties(ENDPOINT);
        checkProperties(DEFAULT_PROJECT);
        checkProperties(ACCESS_ID);
        checkProperties(ACCESS_KEY);
    }

    public String getProperty(String propertyKey) {
        return configs.get(propertyKey);
    }

    @Override
    protected void getProcNodeData(BaseProcResult result) {
        String lowerCaseType = type.name().toLowerCase();
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(ACCESS_KEY)) {
                continue;
            }
            result.addRow(
                    Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
        }
    }

    private void checkProperties(String propertyKey) throws DdlException {
        String value = configs.get(propertyKey);
        if (value == null) {
            throw new DdlException("Missing " + propertyKey + " in properties");
        }
    }
}
