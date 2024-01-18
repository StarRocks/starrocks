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

package com.starrocks.external.table;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Resource;
import com.starrocks.catalog.Table;
import com.starrocks.common.exception.DdlException;
import com.starrocks.server.AbstractTableFactory;
import com.starrocks.server.GlobalStateMgr;

import java.util.Map;

import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.getResourceMappingCatalogName;

public abstract class ExternalTableFactory implements AbstractTableFactory {
    public static final String DB = "database";
    public static final String TABLE = "table";
    public static final String RESOURCE = "resource";
    public static final String PROPERTY_MISSING_MSG =
            "Remote %s is null. Please add properties('%s'='xxx') when create table";

    protected static Table getTableFromResourceMappingCatalog(Map<String, String> properties,
                                                           Table.TableType tableType,
                                                           Resource.ResourceType resourceType) throws DdlException {
        GlobalStateMgr gsm = GlobalStateMgr.getCurrentState();

        if (properties == null) {
            throw new DdlException("Please set properties of " + tableType.name() + " table, "
                    + "they are: database, table and resource");
        }

        Map<String, String> copiedProps = Maps.newHashMap(properties);
        String remoteDbName = copiedProps.get(DB);
        if (Strings.isNullOrEmpty(remoteDbName)) {
            throw new DdlException(String.format(PROPERTY_MISSING_MSG, DB, DB));
        }

        String remoteTableName = copiedProps.get(TABLE);
        if (Strings.isNullOrEmpty(remoteTableName)) {
            throw new DdlException(String.format(PROPERTY_MISSING_MSG, TABLE, TABLE));
        }

        // check properties
        // resource must be set
        String resourceName = copiedProps.get(RESOURCE);
        if (Strings.isNullOrEmpty(resourceName)) {
            throw new DdlException("property " + RESOURCE + " must be set");
        }

        checkResource(resourceName, resourceType);
        String resourceMappingCatalogName = getResourceMappingCatalogName(resourceName, resourceType.name());
        return gsm.getMetadataMgr().getTable(resourceMappingCatalogName, remoteDbName, remoteTableName);
    }

    protected static void checkResource(String resourceName, Resource.ResourceType type) throws DdlException {
        Resource resource = GlobalStateMgr.getCurrentState().getResourceMgr().getResource(resourceName);
        if (resource == null) {
            throw new DdlException(type + " resource [" + resourceName + "] not exists");
        }

        if (resource.getType() != type) {
            throw new DdlException(resource.getType().name() + " resource [" + resourceName + "] is not " + type + " resource");
        }
    }
}
