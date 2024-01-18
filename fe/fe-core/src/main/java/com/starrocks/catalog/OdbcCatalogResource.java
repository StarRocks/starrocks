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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/OdbcCatalogResource.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.exception.DdlException;
import com.starrocks.common.proc.BaseProcResult;

import java.util.Map;

/**
 * External ODBC GlobalStateMgr resource for external table query.
 * <p>
 * External ODBC GlobalStateMgr resource example:
 * CREATE EXTERNAL RESOURCE "odbc_mysql"
 * PROPERTIES
 * (
 * "type" = "external_odbc", [required]
 * "user" = "root", [required]
 * "password" = "root", [required]
 * "host" = "192.168.1.1", [required]
 * "port" = "8086", [required]
 * "odbc_type" = "mysql", [optional, external table of ODBC should set]
 * "driver" = "MySQL driver" [optional, external table of ODBC should set]
 * );
 * <p>
 * DROP RESOURCE "odbc_mysql";
 */
public class OdbcCatalogResource extends Resource {
    // required
    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String USER = "user";
    private static final String PASSWORD = "password";

    // optional
    private static final String TYPE = "odbc_type";
    private static final String DRIVER = "driver";

    @SerializedName(value = "configs")
    private Map<String, String> configs;

    public OdbcCatalogResource(String name) {
        this(name, Maps.newHashMap());
    }

    private OdbcCatalogResource(String name, Map<String, String> configs) {
        super(name, ResourceType.ODBC_CATALOG);
        this.configs = configs;
    }

    public OdbcCatalogResource getCopiedResource() {
        return new OdbcCatalogResource(name, Maps.newHashMap(configs));
    }

    private void checkProperties(String propertieKey) throws DdlException {
        // check the propertie key
        String value = configs.get(propertieKey);
        if (value == null) {
            throw new DdlException("Missing " + propertieKey + " in properties");
        }

    }

    public String getProperties(String propertieKey) {
        // check the propertie key
        String value = configs.get(propertieKey);
        return value;
    }

    @Override
    protected void setProperties(Map<String, String> properties) throws DdlException {
        Preconditions.checkState(properties != null);

        configs = properties;

        checkProperties(HOST);
        checkProperties(PORT);
        checkProperties(USER);
        checkProperties(PASSWORD);
    }

    @Override
    protected void getProcNodeData(BaseProcResult result) {
        String lowerCaseType = type.name().toLowerCase();
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
        }
    }
}

