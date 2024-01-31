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
package com.starrocks.privilege.ranger;

import com.starrocks.privilege.ObjectType;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;

public abstract class RangerAccessResourceBuilder implements ObjectTypeConverter {
    RangerAccessResourceImpl rangerAccessResource;

    public RangerAccessResourceImpl build() {
        return rangerAccessResource;
    }

    protected RangerAccessResourceBuilder(RangerAccessResourceImpl rangerAccessResource) {
        this.rangerAccessResource = rangerAccessResource;
    }

    public RangerAccessResourceBuilder setSystem() {
        rangerAccessResource.setValue(convertToRangerType(ObjectType.SYSTEM), "*");
        return this;
    }

    public RangerAccessResourceBuilder setUser(String user) {
        rangerAccessResource.setValue(convertToRangerType(ObjectType.USER), user);
        return this;
    }

    public RangerAccessResourceBuilder setCatalog(String catalog) {
        rangerAccessResource.setValue(convertToRangerType(ObjectType.CATALOG), catalog);
        return this;
    }

    public RangerAccessResourceBuilder setDatabase(String database) {
        rangerAccessResource.setValue(convertToRangerType(ObjectType.DATABASE), database);
        return this;
    }

    public RangerAccessResourceBuilder setTable(String table) {
        rangerAccessResource.setValue(convertToRangerType(ObjectType.TABLE), table);
        return this;
    }

    public RangerAccessResourceBuilder setColumn(String column) {
        rangerAccessResource.setValue("column", column);
        return this;
    }

    public RangerAccessResourceBuilder setView(String view) {
        rangerAccessResource.setValue(convertToRangerType(ObjectType.VIEW), view);
        return this;
    }

    public RangerAccessResourceBuilder setMaterializedView(String materializedView) {
        rangerAccessResource.setValue(convertToRangerType(ObjectType.MATERIALIZED_VIEW), materializedView);
        return this;
    }

    public RangerAccessResourceBuilder setFunction(String function) {
        rangerAccessResource.setValue(convertToRangerType(ObjectType.FUNCTION), function);
        return this;
    }

    public RangerAccessResourceBuilder setGlobalFunction(String globalFunction) {
        rangerAccessResource.setValue(convertToRangerType(ObjectType.GLOBAL_FUNCTION), globalFunction);
        return this;
    }

    public RangerAccessResourceBuilder setResource(String resource) {
        rangerAccessResource.setValue(convertToRangerType(ObjectType.RESOURCE), resource);
        return this;
    }

    public RangerAccessResourceBuilder setResourceGroup(String resourceGroup) {
        rangerAccessResource.setValue(convertToRangerType(ObjectType.RESOURCE_GROUP), resourceGroup);
        return this;
    }

    public RangerAccessResourceBuilder setStorageVolume(String storageVolume) {
        rangerAccessResource.setValue(convertToRangerType(ObjectType.STORAGE_VOLUME), storageVolume);
        return this;
    }

    public RangerAccessResourceBuilder setPipe(String pipe) {
        rangerAccessResource.setValue(convertToRangerType(ObjectType.PIPE), pipe);
        return this;
    }
}
