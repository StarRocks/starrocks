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

import com.starrocks.privilege.ObjectType;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;

import java.util.List;
import java.util.Locale;

public class RangerStarRocksResource extends RangerAccessResourceImpl {
    public RangerStarRocksResource(ObjectType objectType, List<String> objectTokens) {
        if (objectType.equals(ObjectType.CATALOG)) {
            setValue(ObjectType.CATALOG.name().toLowerCase(Locale.ENGLISH), objectTokens.get(0));
        } else if (objectType.equals(ObjectType.DATABASE)) {
            setValue(ObjectType.CATALOG.name().toLowerCase(Locale.ENGLISH), objectTokens.get(0));
            setValue(ObjectType.DATABASE.name().toLowerCase(Locale.ENGLISH), objectTokens.get(1));
        } else if (objectType.equals(ObjectType.TABLE)) {
            setValue(ObjectType.CATALOG.name().toLowerCase(Locale.ENGLISH), objectTokens.get(0));
            setValue(ObjectType.DATABASE.name().toLowerCase(Locale.ENGLISH), objectTokens.get(1));
            setValue(ObjectType.TABLE.name().toLowerCase(Locale.ENGLISH), objectTokens.get(2));
        } else if (objectType.equals(ObjectType.VIEW)) {
            setValue(ObjectType.DATABASE.name().toLowerCase(Locale.ENGLISH), objectTokens.get(0));
            setValue(ObjectType.VIEW.name().toLowerCase(Locale.ENGLISH), objectTokens.get(1));
        } else if (objectType.equals(ObjectType.MATERIALIZED_VIEW)) {
            setValue(ObjectType.DATABASE.name().toLowerCase(Locale.ENGLISH), objectTokens.get(0));
            setValue(ObjectType.MATERIALIZED_VIEW.name().toLowerCase(Locale.ENGLISH), objectTokens.get(1));
        } else if (objectType.equals(ObjectType.FUNCTION)) {
            setValue(ObjectType.DATABASE.name().toLowerCase(Locale.ENGLISH), objectTokens.get(0));
            setValue(ObjectType.FUNCTION.name().toLowerCase(Locale.ENGLISH), objectTokens.get(1));
        } else if (objectType.equals(ObjectType.GLOBAL_FUNCTION)) {
            setValue(ObjectType.GLOBAL_FUNCTION.name().toLowerCase(Locale.ENGLISH), objectTokens.get(0));
        } else if (objectType.equals(ObjectType.RESOURCE)) {
            setValue(ObjectType.RESOURCE.name().toLowerCase(Locale.ENGLISH), objectTokens.get(0));
        } else if (objectType.equals(ObjectType.RESOURCE_GROUP)) {
            setValue(ObjectType.RESOURCE_GROUP.name().toLowerCase(Locale.ENGLISH), objectTokens.get(0));
        } else if (objectType.equals(ObjectType.STORAGE_VOLUME)) {
            setValue(ObjectType.STORAGE_VOLUME.name().toLowerCase(Locale.ENGLISH), objectTokens.get(0));
        }
    }

    public RangerStarRocksResource(String catalogName, String dbName, String tableName, String columnName) {
        setValue(ObjectType.CATALOG.name().toLowerCase(Locale.ENGLISH), catalogName);
        setValue(ObjectType.DATABASE.name().toLowerCase(Locale.ENGLISH), dbName);
        setValue(ObjectType.TABLE.name().toLowerCase(Locale.ENGLISH), tableName);
        setValue("column", columnName);
    }
}