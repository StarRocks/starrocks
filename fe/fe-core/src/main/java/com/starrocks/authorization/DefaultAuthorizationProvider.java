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

import com.google.common.collect.Lists;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.UserIdentity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DefaultAuthorizationProvider implements AuthorizationProvider {
    private static final short PLUGIN_ID = 1;
    private static final short PLUGIN_VERSION = 1;

    protected final Map<ObjectType, List<PrivilegeType>> typeToActionList = new HashMap<>();

    public DefaultAuthorizationProvider() {
        typeToActionList.put(ObjectType.TABLE, Lists.newArrayList(
                PrivilegeType.DELETE,
                PrivilegeType.DROP,
                PrivilegeType.INSERT,
                PrivilegeType.SELECT,
                PrivilegeType.ALTER,
                PrivilegeType.EXPORT,
                PrivilegeType.UPDATE));

        typeToActionList.put(ObjectType.DATABASE, Lists.newArrayList(
                PrivilegeType.CREATE_TABLE,
                PrivilegeType.DROP,
                PrivilegeType.ALTER,
                PrivilegeType.CREATE_VIEW,
                PrivilegeType.CREATE_FUNCTION,
                PrivilegeType.CREATE_MATERIALIZED_VIEW,
                PrivilegeType.CREATE_PIPE));

        typeToActionList.put(ObjectType.SYSTEM, Lists.newArrayList(
                PrivilegeType.GRANT,
                PrivilegeType.NODE,
                PrivilegeType.CREATE_RESOURCE,
                PrivilegeType.PLUGIN,
                PrivilegeType.FILE,
                PrivilegeType.BLACKLIST,
                PrivilegeType.OPERATE,
                PrivilegeType.CREATE_EXTERNAL_CATALOG,
                PrivilegeType.REPOSITORY,
                PrivilegeType.CREATE_RESOURCE_GROUP,
                PrivilegeType.CREATE_GLOBAL_FUNCTION,
                PrivilegeType.CREATE_STORAGE_VOLUME,
                PrivilegeType.CREATE_WAREHOUSE));

        typeToActionList.put(ObjectType.USER, Lists.newArrayList(
                PrivilegeType.IMPERSONATE));

        typeToActionList.put(ObjectType.RESOURCE, Lists.newArrayList(
                PrivilegeType.USAGE,
                PrivilegeType.ALTER,
                PrivilegeType.DROP));

        typeToActionList.put(ObjectType.VIEW, Lists.newArrayList(
                PrivilegeType.SELECT,
                PrivilegeType.ALTER,
                PrivilegeType.DROP));

        typeToActionList.put(ObjectType.CATALOG, Lists.newArrayList(
                PrivilegeType.USAGE,
                PrivilegeType.CREATE_DATABASE,
                PrivilegeType.DROP,
                PrivilegeType.ALTER));

        typeToActionList.put(ObjectType.MATERIALIZED_VIEW, Lists.newArrayList(
                PrivilegeType.ALTER,
                PrivilegeType.REFRESH,
                PrivilegeType.DROP,
                PrivilegeType.SELECT));

        typeToActionList.put(ObjectType.FUNCTION, Lists.newArrayList(
                PrivilegeType.USAGE,
                PrivilegeType.DROP));

        typeToActionList.put(ObjectType.RESOURCE_GROUP, Lists.newArrayList(
                PrivilegeType.ALTER,
                PrivilegeType.DROP));

        typeToActionList.put(ObjectType.PIPE, Lists.newArrayList(
                PrivilegeType.ALTER,
                PrivilegeType.DROP,
                PrivilegeType.USAGE));

        typeToActionList.put(ObjectType.GLOBAL_FUNCTION, Lists.newArrayList(
                PrivilegeType.USAGE,
                PrivilegeType.DROP));

        typeToActionList.put(ObjectType.STORAGE_VOLUME, Lists.newArrayList(
                PrivilegeType.DROP,
                PrivilegeType.ALTER,
                PrivilegeType.USAGE));

        typeToActionList.put(ObjectType.WAREHOUSE, Lists.newArrayList(
                PrivilegeType.USAGE,
                PrivilegeType.ALTER,
                PrivilegeType.DROP));
    }

    public static final String UNEXPECTED_TYPE = "unexpected type ";

    @Override
    public short getPluginId() {
        return PLUGIN_ID;
    }

    @Override
    public short getPluginVersion() {
        return PLUGIN_VERSION;
    }

    @Override
    public Set<ObjectType> getAllPrivObjectTypes() {
        return typeToActionList.keySet();
    }

    @Override
    public List<PrivilegeType> getAvailablePrivType(ObjectType objectType) {
        return new ArrayList<>(typeToActionList.get(objectType));
    }

    @Override
    public boolean isAvailablePrivType(ObjectType objectType, PrivilegeType privilegeType) {
        if (!typeToActionList.containsKey(objectType)) {
            return false;
        }
        return typeToActionList.get(objectType).contains(privilegeType);
    }

    @Override
    public PrivilegeType getPrivilegeType(String privTypeString) {
        return PrivilegeType.NAME_TO_PRIVILEGE.get(privTypeString);
    }

    @Override
    public ObjectType getObjectType(String objectTypeUnResolved) {
        if (ObjectType.NAME_TO_OBJECT.containsKey(objectTypeUnResolved)) {
            return ObjectType.NAME_TO_OBJECT.get(objectTypeUnResolved);
        }

        if (ObjectType.PLURAL_TO_OBJECT.containsKey(objectTypeUnResolved)) {
            return ObjectType.PLURAL_TO_OBJECT.get(objectTypeUnResolved);
        }

        throw new SemanticException("cannot find privilege object type " + objectTypeUnResolved);
    }

    @Override
    public PEntryObject generateObject(ObjectType objectType, List<String> objectTokens)
            throws PrivilegeException {
        if (ObjectType.TABLE.equals(objectType)) {
            return TablePEntryObject.generate(objectTokens);
        } else if (ObjectType.DATABASE.equals(objectType)) {
            return DbPEntryObject.generate(objectTokens);
        } else if (ObjectType.RESOURCE.equals(objectType)) {
            return ResourcePEntryObject.generate(objectTokens);
        } else if (ObjectType.VIEW.equals(objectType)) {
            return ViewPEntryObject.generate(objectTokens);
        } else if (ObjectType.MATERIALIZED_VIEW.equals(objectType)) {
            return MaterializedViewPEntryObject.generate(objectTokens);
        } else if (ObjectType.CATALOG.equals(objectType)) {
            return CatalogPEntryObject.generate(objectTokens);
        } else if (ObjectType.RESOURCE_GROUP.equals(objectType)) {
            return ResourceGroupPEntryObject.generate(objectTokens);
        } else if (ObjectType.STORAGE_VOLUME.equals(objectType)) {
            return StorageVolumePEntryObject.generate(objectTokens);
        } else if (ObjectType.PIPE.equals(objectType)) {
            return PipePEntryObject.generate(objectTokens);
        } else if (ObjectType.WAREHOUSE.equals(objectType)) {
            return WarehousePEntryObject.generate(objectTokens);
        }
        throw new PrivilegeException(UNEXPECTED_TYPE + objectType.name());
    }

    @Override
    public PEntryObject generateUserObject(
            ObjectType objectType, UserIdentity user) throws PrivilegeException {
        if (objectType.equals(ObjectType.USER)) {
            return UserPEntryObject.generate(user);
        }
        throw new PrivilegeException(UNEXPECTED_TYPE + objectType.name());
    }

    @Override
    public PEntryObject generateFunctionObject(ObjectType objectType, Long databaseId, Long functionId)
            throws PrivilegeException {
        if (objectType.equals(ObjectType.FUNCTION) || objectType.equals(ObjectType.GLOBAL_FUNCTION)) {
            return FunctionPEntryObject.generate(databaseId, functionId);
        }
        throw new PrivilegeException(UNEXPECTED_TYPE + objectType.name());
    }

    private static final List<PrivilegeType> BAD_SYSTEM_ACTIONS = Arrays.asList(PrivilegeType.GRANT, PrivilegeType.NODE);

    @Override
    public void validateGrant(ObjectType objectType, List<PrivilegeType> privilegeTypes, List<PEntryObject> objects)
            throws PrivilegeException {
        if (objectType.equals(ObjectType.SYSTEM)) {
            for (PrivilegeType badAction : BAD_SYSTEM_ACTIONS) {
                if (privilegeTypes.contains(badAction)) {
                    throw new PrivilegeException("Operation not permitted, '" + badAction.toString() +
                            "' cannot be granted to user or role directly, use built-in role instead");
                }
            }
        }
    }

    @Override
    public boolean check(ObjectType objectType, PrivilegeType want, PEntryObject object, PrivilegeCollectionV2
            currentPrivilegeCollection) {
        return currentPrivilegeCollection.check(objectType, want, object);
    }

    @Override
    public boolean searchAnyActionOnObject(ObjectType objectType, PEntryObject object,
                                           PrivilegeCollectionV2 currentPrivilegeCollection) {
        return currentPrivilegeCollection.searchAnyActionOnObject(objectType, object);
    }

    @Override
    public boolean searchActionOnObject(ObjectType objectType, PEntryObject object,
                                        PrivilegeCollectionV2 currentPrivilegeCollection, PrivilegeType want) {
        return currentPrivilegeCollection.searchActionOnObject(objectType, object, want);
    }

    @Override
    public boolean allowGrant(ObjectType objectType, List<PrivilegeType> wants, List<PEntryObject> objects,
                              PrivilegeCollectionV2 currentPrivilegeCollection) {
        return currentPrivilegeCollection.allowGrant(objectType, wants, objects);
    }
}