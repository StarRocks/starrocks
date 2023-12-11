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

package com.starrocks.privilege.ranger.starrocks;

import com.starrocks.catalog.InternalCatalog;
import com.starrocks.privilege.ObjectType;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;

import java.util.List;

public class RangerStarRocksResource extends RangerAccessResourceImpl {
    public RangerStarRocksResource(ObjectType objectType, List<String> objectTokens) {
        if (objectType.equals(ObjectType.SYSTEM)) {
            setValue(convertToRangerType(ObjectType.SYSTEM), "*");
        } else if (objectType.equals(ObjectType.USER)) {
            setValue(convertToRangerType(ObjectType.USER), objectTokens.get(0));
        } else if (objectType.equals(ObjectType.CATALOG)) {
            setValue(convertToRangerType(ObjectType.CATALOG), objectTokens.get(0));
        } else if (objectType.equals(ObjectType.DATABASE)) {
            setValue(convertToRangerType(ObjectType.CATALOG), objectTokens.get(0));
            setValue(convertToRangerType(ObjectType.DATABASE), objectTokens.get(1));
        } else if (objectType.equals(ObjectType.TABLE)) {
            setValue(convertToRangerType(ObjectType.CATALOG), objectTokens.get(0));
            setValue(convertToRangerType(ObjectType.DATABASE), objectTokens.get(1));
            setValue(convertToRangerType(ObjectType.TABLE), objectTokens.get(2));
        } else if (objectType.equals(ObjectType.VIEW)) {
            setValue(convertToRangerType(ObjectType.CATALOG), InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
            setValue(convertToRangerType(ObjectType.DATABASE), objectTokens.get(0));
            setValue(convertToRangerType(ObjectType.VIEW), objectTokens.get(1));
        } else if (objectType.equals(ObjectType.MATERIALIZED_VIEW)) {
            setValue(convertToRangerType(ObjectType.CATALOG), InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
            setValue(convertToRangerType(ObjectType.DATABASE), objectTokens.get(0));
            setValue(convertToRangerType(ObjectType.MATERIALIZED_VIEW), objectTokens.get(1));
        } else if (objectType.equals(ObjectType.FUNCTION)) {
            setValue(convertToRangerType(ObjectType.CATALOG), InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
            setValue(convertToRangerType(ObjectType.DATABASE), objectTokens.get(0));
            setValue(convertToRangerType(ObjectType.FUNCTION), objectTokens.get(1));
        } else if (objectType.equals(ObjectType.GLOBAL_FUNCTION)) {
            setValue(convertToRangerType(ObjectType.GLOBAL_FUNCTION), objectTokens.get(0));
        } else if (objectType.equals(ObjectType.RESOURCE)) {
            setValue(convertToRangerType(ObjectType.RESOURCE), objectTokens.get(0));
        } else if (objectType.equals(ObjectType.RESOURCE_GROUP)) {
            setValue(convertToRangerType(ObjectType.RESOURCE_GROUP), objectTokens.get(0));
        } else if (objectType.equals(ObjectType.STORAGE_VOLUME)) {
            setValue(convertToRangerType(ObjectType.STORAGE_VOLUME), objectTokens.get(0));
        } else if (objectType.equals(ObjectType.PIPE)) {
            setValue(convertToRangerType(ObjectType.PIPE), objectTokens.get(0));
        }
    }

    public RangerStarRocksResource(String catalogName, String dbName, String tableName, String columnName) {
        setValue(convertToRangerType(ObjectType.CATALOG), catalogName);
        setValue(convertToRangerType(ObjectType.DATABASE), dbName);
        setValue(convertToRangerType(ObjectType.TABLE), tableName);
        setValue("column", columnName);
    }

    private String convertToRangerType(ObjectType objectType) {
        if (objectType.equals(ObjectType.SYSTEM)) {
            return "system";
        } else if (objectType.equals(ObjectType.USER)) {
            return "user";
        } else if (objectType.equals(ObjectType.CATALOG)) {
            return "catalog";
        } else if (objectType.equals(ObjectType.DATABASE)) {
            return "database";
        } else if (objectType.equals(ObjectType.TABLE)) {
            return "table";
        } else if (objectType.equals(ObjectType.VIEW)) {
            return "view";
        } else if (objectType.equals(ObjectType.MATERIALIZED_VIEW)) {
            return "materialized_view";
        } else if (objectType.equals(ObjectType.FUNCTION)) {
            return "function";
        } else if (objectType.equals(ObjectType.GLOBAL_FUNCTION)) {
            return "global_function";
        } else if (objectType.equals(ObjectType.RESOURCE)) {
            return "resource";
        } else if (objectType.equals(ObjectType.RESOURCE_GROUP)) {
            return "resource_group";
        } else if (objectType.equals(ObjectType.STORAGE_VOLUME)) {
            return "storage_volume";
        } else if (objectType.equals(ObjectType.PIPE)) {
            return "pipe";
        } else {
            return "unknown";
        }
    }
}