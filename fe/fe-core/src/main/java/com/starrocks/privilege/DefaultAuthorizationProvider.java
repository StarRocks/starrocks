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

package com.starrocks.privilege;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.common.Pair;
import com.starrocks.server.GlobalStateMgr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DefaultAuthorizationProvider implements AuthorizationProvider {
    private static final short PLUGIN_ID = 1;
    private static final short PLUGIN_VERSION = 1;

    private static final Map<ObjectType, List<PrivilegeType>> TYPE_TO_ACTION_LIST =
            ImmutableMap.<ObjectType, List<PrivilegeType>>builder()
                    .put(ObjectType.TABLE, ImmutableList.of(
                            PrivilegeType.DELETE,
                            PrivilegeType.DROP,
                            PrivilegeType.INSERT,
                            PrivilegeType.SELECT,
                            PrivilegeType.ALTER,
                            PrivilegeType.EXPORT,
                            PrivilegeType.UPDATE))

                    .put(ObjectType.DATABASE, ImmutableList.of(
                            PrivilegeType.CREATE_TABLE,
                            PrivilegeType.DROP,
                            PrivilegeType.ALTER,
                            PrivilegeType.CREATE_VIEW,
                            PrivilegeType.CREATE_FUNCTION,
                            PrivilegeType.CREATE_MATERIALIZED_VIEW))

                    .put(ObjectType.SYSTEM, ImmutableList.of(
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
                            PrivilegeType.CREATE_GLOBAL_FUNCTION))

                    .put(ObjectType.USER, ImmutableList.of(
                            PrivilegeType.IMPERSONATE))

                    .put(ObjectType.RESOURCE, ImmutableList.of(
                            PrivilegeType.USAGE,
                            PrivilegeType.ALTER,
                            PrivilegeType.DROP))

                    .put(ObjectType.VIEW, ImmutableList.of(
                            PrivilegeType.SELECT,
                            PrivilegeType.ALTER,
                            PrivilegeType.DROP))

                    .put(ObjectType.CATALOG, ImmutableList.of(
                            PrivilegeType.USAGE,
                            PrivilegeType.CREATE_DATABASE,
                            PrivilegeType.DROP,
                            PrivilegeType.ALTER))

                    .put(ObjectType.MATERIALIZED_VIEW, ImmutableList.of(
                            PrivilegeType.ALTER,
                            PrivilegeType.REFRESH,
                            PrivilegeType.DROP,
                            PrivilegeType.SELECT))

                    .put(ObjectType.FUNCTION, ImmutableList.of(
                            PrivilegeType.USAGE,
                            PrivilegeType.DROP))

                    .put(ObjectType.RESOURCE_GROUP, ImmutableList.of(
                            PrivilegeType.ALTER,
                            PrivilegeType.DROP))

                    .put(ObjectType.GLOBAL_FUNCTION, ImmutableList.of(
                            PrivilegeType.USAGE,
                            PrivilegeType.DROP))
                    .build();

    private static final List<Pair<ObjectType, String>> OBJECT_TO_PLURAL = ImmutableList.of(
            new Pair<>(ObjectType.TABLE, "TABLES"),
            new Pair<>(ObjectType.DATABASE, "DATABASES"),
            new Pair<>(ObjectType.SYSTEM, null),
            new Pair<>(ObjectType.USER, "USERS"),
            new Pair<>(ObjectType.RESOURCE, "RESOURCES"),
            new Pair<>(ObjectType.VIEW, "VIEWS"),
            new Pair<>(ObjectType.CATALOG, "CATALOGS"),
            new Pair<>(ObjectType.MATERIALIZED_VIEW, "MATERIALIZED_VIEWS"),
            new Pair<>(ObjectType.FUNCTION, "FUNCTIONS"),
            new Pair<>(ObjectType.RESOURCE_GROUP, "RESOURCE_GROUPS"),
            new Pair<>(ObjectType.GLOBAL_FUNCTION, "GLOBAL_FUNCTIONS")
    );

    private static final Map<String, ObjectType> PLURAL_TO_TYPE = new HashMap<>();
    private static final Map<ObjectType, String> TYPE_TO_PLURAL = new HashMap<>();

    static {
        for (Pair<ObjectType, String> pair : OBJECT_TO_PLURAL) {
            TYPE_TO_PLURAL.put(pair.first, pair.second);
            PLURAL_TO_TYPE.put(pair.second, pair.first);
        }
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
        return TYPE_TO_ACTION_LIST.keySet();
    }

    @Override
    public List<PrivilegeType> getAvailablePrivType(ObjectType objectType) {
        return new ArrayList<>(TYPE_TO_ACTION_LIST.get(objectType));
    }

    @Override
    public boolean isAvailablePrivType(ObjectType objectType, PrivilegeType privilegeType) {
        if (!TYPE_TO_ACTION_LIST.containsKey(objectType)) {
            return false;
        }
        return TYPE_TO_ACTION_LIST.get(objectType).contains(privilegeType);
    }


    @Override
    public ObjectType getTypeNameByPlural(String plural) throws PrivilegeException {
        ObjectType ret = PLURAL_TO_TYPE.get(plural);
        if (ret == null) {
            throw new PrivilegeException("invalid plural privilege type " + plural);
        }
        return ret;
    }

    @Override
    public String getPlural(ObjectType objectType) throws PrivilegeException {
        String plural = TYPE_TO_PLURAL.get(objectType);
        if (plural == null) {
            throw new PrivilegeException("invalid plural privilege type " + plural);
        }
        return plural;
    }


    @Override
    public PEntryObject generateObject(ObjectType objectType, List<String> objectTokens, GlobalStateMgr mgr)
            throws PrivilegeException {
        switch (objectType) {
            case TABLE:
                return TablePEntryObject.generate(mgr, objectTokens);

            case DATABASE:
                return DbPEntryObject.generate(mgr, objectTokens);

            case RESOURCE:
                return ResourcePEntryObject.generate(mgr, objectTokens);

            case VIEW:
                return ViewPEntryObject.generate(mgr, objectTokens);

            case MATERIALIZED_VIEW:
                return MaterializedViewPEntryObject.generate(mgr, objectTokens);

            case CATALOG:
                return CatalogPEntryObject.generate(mgr, objectTokens);

            case FUNCTION:
                return FunctionPEntryObject.generate(mgr, objectTokens);

            case RESOURCE_GROUP:
                return ResourceGroupPEntryObject.generate(mgr, objectTokens);

            case GLOBAL_FUNCTION:
                return GlobalFunctionPEntryObject.generate(mgr, objectTokens);

            default:
                throw new PrivilegeException(UNEXPECTED_TYPE + objectType.name());
        }
    }

    @Override
    public PEntryObject generateUserObject(
            ObjectType objectType, UserIdentity user, GlobalStateMgr globalStateMgr) throws PrivilegeException {
        if (objectType.equals(ObjectType.USER)) {
            return UserPEntryObject.generate(globalStateMgr, user);
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
                    throw new PrivilegeException("cannot grant/revoke system privilege: " + badAction);
                }
            }
        }
    }

    @Override
    public boolean check(ObjectType objectType, PrivilegeType want, PEntryObject object, PrivilegeCollection
            currentPrivilegeCollection) {
        return currentPrivilegeCollection.check(objectType, want, object);
    }

    @Override
    public boolean searchAnyActionOnObject(ObjectType objectType, PEntryObject object,
                                           PrivilegeCollection currentPrivilegeCollection) {
        return currentPrivilegeCollection.searchAnyActionOnObject(objectType, object);
    }

    @Override
    public boolean searchActionOnObject(ObjectType objectType, PEntryObject object,
                                        PrivilegeCollection currentPrivilegeCollection, PrivilegeType want) {
        return currentPrivilegeCollection.searchActionOnObject(objectType, object, want);
    }

    @Override
    public boolean allowGrant(ObjectType objectType, List<PrivilegeType> wants, List<PEntryObject> objects,
                              PrivilegeCollection currentPrivilegeCollection) {
        return currentPrivilegeCollection.allowGrant(objectType, wants, objects);
    }

    @Override
    public void upgradePrivilegeCollection(PrivilegeCollection info, short pluginId, short metaVersion)
            throws PrivilegeException {
        if (pluginId != PLUGIN_ID && metaVersion != PLUGIN_VERSION) {
            throw new PrivilegeException(String.format(
                    "unexpected privilege collection %s; plugin id expect %d actual %d; version expect %d actual %d",
                    info.toString(), PLUGIN_ID, pluginId, PLUGIN_VERSION, metaVersion));
        }
    }
}
