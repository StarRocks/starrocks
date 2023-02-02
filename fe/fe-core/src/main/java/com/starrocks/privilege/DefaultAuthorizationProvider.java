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

import com.google.common.collect.Sets;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.server.GlobalStateMgr;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DefaultAuthorizationProvider implements AuthorizationProvider {
    private static final short PLUGIN_ID = 1;
    private static final short PLUGIN_VERSION = 1;

    private static final Map<Short, ObjectType> ID_TO_OBJECT_TYPE = new HashMap<>();
    private static final Map<Short, Map<String, Action>> TYPE_TO_ACTION_MAP = new HashMap<>();
    private static final Map<String, String> PLURAL_TO_TYPE = new HashMap<>();
    public static final String UNEXPECTED_TYPE = "unexpected type ";

    static {
        for (ObjectType type : ObjectType.values()) {
            ID_TO_OBJECT_TYPE.put((short) type.getId(), type);
            TYPE_TO_ACTION_MAP.put((short) type.getId(), type.getActionMap());
            PLURAL_TO_TYPE.put(type.getPlural(), type.toString());
        }
    }

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
        return Sets.newHashSet(ObjectType.values());
    }

    @Override
    public ObjectType getObjectType(short typeId) throws PrivilegeException {
        ObjectType privilegeType = ID_TO_OBJECT_TYPE.get(typeId);
        if (privilegeType == null) {
            throw new PrivilegeException("cannot find typeId " + typeId + " in " + ID_TO_OBJECT_TYPE.keySet());
        } else {
            return privilegeType;
        }
    }

    @Override
    public Collection<Action> getAllActions(short typeId) throws PrivilegeException {
        Map<String, Action> actionMap = TYPE_TO_ACTION_MAP.get(typeId);
        if (actionMap == null) {
            throw new PrivilegeException("cannot find type " + ID_TO_OBJECT_TYPE.get(typeId) +
                    " in " + TYPE_TO_ACTION_MAP.keySet());
        }
        return actionMap.values();
    }

    @Override
    public Action getAction(short objectTypeId, String actionName) throws PrivilegeException {
        Map<String, Action> actionMap = TYPE_TO_ACTION_MAP.get(objectTypeId);
        if (actionMap == null) {
            throw new PrivilegeException("cannot find type " + ID_TO_OBJECT_TYPE.get(objectTypeId) +
                    " in " + TYPE_TO_ACTION_MAP.keySet());
        }
        Action action = actionMap.get(actionName);
        if (action == null) {
            throw new PrivilegeException("cannot find action " + actionName + " in " + actionMap.keySet() +
                    " on object type " + ID_TO_OBJECT_TYPE.get(objectTypeId).name().toUpperCase());
        }
        return action;
    }

    @Override
    public String getTypeNameByPlural(String plural) throws PrivilegeException {
        String ret = PLURAL_TO_TYPE.get(plural);
        if (ret == null) {
            throw new PrivilegeException("invalid plural privilege type " + plural);
        }
        return ret;
    }

    @Override
    public PEntryObject generateObject(String typeStr, List<String> objectTokens, GlobalStateMgr mgr)
            throws PrivilegeException {
        ObjectType type = ObjectType.valueOf(typeStr);
        switch (type) {
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
                throw new PrivilegeException(UNEXPECTED_TYPE + typeStr);
        }
    }

    @Override
    public PEntryObject generateUserObject(
            String typeStr, UserIdentity user, GlobalStateMgr globalStateMgr) throws PrivilegeException {
        if (typeStr.equals("USER")) {
            return UserPEntryObject.generate(globalStateMgr, user);
        }
        throw new PrivilegeException(UNEXPECTED_TYPE + typeStr);
    }

    @Override
    public PEntryObject generateObject(
            String typeStr, List<String> allTypes, String restrictType, String restrictName, GlobalStateMgr mgr)
            throws PrivilegeException {
        ObjectType type = ObjectType.valueOf(typeStr);
        switch (type) {
            case TABLE:
                return TablePEntryObject.generate(mgr, allTypes, restrictType, restrictName);

            case DATABASE:
                return DbPEntryObject.generate(allTypes, restrictType, restrictName);

            case USER:
                return UserPEntryObject.generate(allTypes, restrictType, restrictName);

            case RESOURCE:
                return ResourcePEntryObject.generate(allTypes, restrictType, restrictName);

            case VIEW:
                return ViewPEntryObject.generate(mgr, allTypes, restrictType, restrictName);

            case MATERIALIZED_VIEW:
                return MaterializedViewPEntryObject.generate(mgr, allTypes, restrictType, restrictName);

            case CATALOG:
                return CatalogPEntryObject.generate(allTypes, restrictType, restrictName);

            case FUNCTION:
                return FunctionPEntryObject.generate(mgr, allTypes, restrictType, restrictName);

            case RESOURCE_GROUP:
                return ResourceGroupPEntryObject.generate(allTypes, restrictType, restrictName);

            case GLOBAL_FUNCTION:
                return GlobalFunctionPEntryObject.generate(mgr, allTypes, restrictType, restrictName);

            default:
                throw new PrivilegeException(UNEXPECTED_TYPE + typeStr);
        }
    }

    private static final List<String> BAD_SYSTEM_ACTIONS = Arrays.asList("GRANT", "NODE");

    @Override
    public void validateGrant(String type, List<String> actions, List<PEntryObject> objects) throws PrivilegeException {
        if (type.equals("SYSTEM")) {
            for (String badAction : BAD_SYSTEM_ACTIONS) {
                if (actions.contains(badAction)) {
                    throw new PrivilegeException("cannot grant/revoke system privilege: " + badAction);
                }
            }
        }
    }

    @Override
    public boolean check(short type, Action want, PEntryObject object, PrivilegeCollection currentPrivilegeCollection) {
        return currentPrivilegeCollection.check(type, want, object);
    }

    @Override
    public boolean searchAnyActionOnObject(short type, PEntryObject object,
                                           PrivilegeCollection currentPrivilegeCollection) {
        return currentPrivilegeCollection.searchAnyActionOnObject(type, object);
    }

    @Override
    public boolean searchActionOnObject(short type, PEntryObject object,
                                        PrivilegeCollection currentPrivilegeCollection, Action want) {
        return currentPrivilegeCollection.searchActionOnObject(type, object, want);
    }

    @Override
    public boolean allowGrant(
            short type, ActionSet wants, List<PEntryObject> objects, PrivilegeCollection currentPrivilegeCollection) {
        return currentPrivilegeCollection.allowGrant(type, wants, objects);
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
