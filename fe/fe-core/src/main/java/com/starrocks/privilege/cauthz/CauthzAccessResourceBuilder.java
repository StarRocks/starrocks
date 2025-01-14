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
package com.starrocks.privilege.cauthz;

import com.starrocks.privilege.ObjectType;
import com.starrocks.privilege.cauthz.CauthzAccessResourceImpl;

/**
 * CauthzAccessResourceBuilder is a builder for CauthzAccessResourceImpl
 * It provices setter methods specific to each type of object that can compose a 
 * a resource.
 */

public abstract class CauthzAccessResourceBuilder implements ObjectTypeConverter {
    CauthzAccessResourceImpl cauthzAccessResource;

    public CauthzAccessResourceImpl build() {
        return cauthzAccessResource;
    }

    protected CauthzAccessResourceBuilder(CauthzAccessResourceImpl cauthzAccessResource) {
        this.cauthzAccessResource = cauthzAccessResource;
    }

    public CauthzAccessResourceBuilder setSystem() {
        cauthzAccessResource.setValue(convertToCauthzObjectType(ObjectType.SYSTEM), "*");
        return this;
    }

    public CauthzAccessResourceBuilder setUser(String user) {
        cauthzAccessResource.setValue(convertToCauthzObjectType(ObjectType.USER), user);
        return this;
    }

    public CauthzAccessResourceBuilder setCatalog(String catalog) {
        cauthzAccessResource.setValue(convertToCauthzObjectType(ObjectType.CATALOG), catalog);
        return this;
    }

    public CauthzAccessResourceBuilder setDatabase(String database) {
        cauthzAccessResource.setValue(convertToCauthzObjectType(ObjectType.DATABASE), database);
        return this;
    }

    public CauthzAccessResourceBuilder setTable(String table) {
        cauthzAccessResource.setValue(convertToCauthzObjectType(ObjectType.TABLE), table);
        return this;
    }

    public CauthzAccessResourceBuilder setColumn(String column) {
        cauthzAccessResource.setValue("column", column);
        return this;
    }

    public CauthzAccessResourceBuilder setView(String view) {
        cauthzAccessResource.setValue(convertToCauthzObjectType(ObjectType.VIEW), view);
        return this;
    }

    public CauthzAccessResourceBuilder setMaterializedView(String materializedView) {
        cauthzAccessResource.setValue(convertToCauthzObjectType(ObjectType.MATERIALIZED_VIEW), materializedView);
        return this;
    }

    public CauthzAccessResourceBuilder setFunction(String function) {
        cauthzAccessResource.setValue(convertToCauthzObjectType(ObjectType.FUNCTION), function);
        return this;
    }

    public CauthzAccessResourceBuilder setGlobalFunction(String globalFunction) {
        cauthzAccessResource.setValue(convertToCauthzObjectType(ObjectType.GLOBAL_FUNCTION), globalFunction);
        return this;
    }

    public CauthzAccessResourceBuilder setResource(String resource) {
        cauthzAccessResource.setValue(convertToCauthzObjectType(ObjectType.RESOURCE), resource);
        return this;
    }

    public CauthzAccessResourceBuilder setResourceGroup(String resourceGroup) {
        cauthzAccessResource.setValue(convertToCauthzObjectType(ObjectType.RESOURCE_GROUP), resourceGroup);
        return this;
    }

    public CauthzAccessResourceBuilder setStorageVolume(String storageVolume) {
        cauthzAccessResource.setValue(convertToCauthzObjectType(ObjectType.STORAGE_VOLUME), storageVolume);
        return this;
    }

    public CauthzAccessResourceBuilder setPipe(String pipe) {
        cauthzAccessResource.setValue(convertToCauthzObjectType(ObjectType.PIPE), pipe);
        return this;
    }
}