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

package com.starrocks.authz.authorization;

import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;

import java.util.List;
import java.util.Set;

public interface AuthorizationProvider {

    /**
     * return plugin id & version
     */
    short getPluginId();

    short getPluginVersion();

    Set<ObjectType> getAllPrivObjectTypes();

    List<PrivilegeType> getAvailablePrivType(ObjectType objectType);

    boolean isAvailablePrivType(ObjectType objectType, PrivilegeType privilegeType);

    /**
     * generate PEntryObject by tokenlist
     */
    PEntryObject generateObject(ObjectType objectType, List<String> objectTokens, GlobalStateMgr mgr) throws PrivilegeException;

    PEntryObject generateUserObject(ObjectType objectType, UserIdentity user, GlobalStateMgr mgr) throws PrivilegeException;

    PEntryObject generateFunctionObject(ObjectType objectType, Long databaseId, Long functionId, GlobalStateMgr globalStateMgr)
            throws PrivilegeException;

    /**
     * validate if grant is allowed
     * e.g. To forbid `NODE` privilege being granted, we should put some code here.
     */
    void validateGrant(
            ObjectType objectType,
            List<PrivilegeType> privilegeTypes,
            List<PEntryObject> objects) throws PrivilegeException;

    /**
     * check if certain action of certain type is allowed on certain object.
     * Developers can implement their own logic here.
     */
    boolean check(
            ObjectType objectType,
            PrivilegeType want,
            PEntryObject object,
            PrivilegeCollectionV2 currentPrivilegeCollection);

    /**
     * Search if any object in collection matches the specified object, any action is ok.
     * The specified object can be fuzzy-matching object.
     * For example, `use db1` statement will pass a (db1, ALL) as the object to check if any table exists
     */
    boolean searchAnyActionOnObject(
            ObjectType objectType,
            PEntryObject object,
            PrivilegeCollectionV2 currentPrivilegeCollection);

    /**
     * Search if any object in collection matches the specified object with required action.
     */
    boolean searchActionOnObject(
            ObjectType objectType,
            PEntryObject object,
            PrivilegeCollectionV2 currentPrivilegeCollection,
            PrivilegeType want);

    boolean allowGrant(
            ObjectType objectType,
            List<PrivilegeType> wants,
            List<PEntryObject> objects,
            PrivilegeCollectionV2 currentPrivilegeCollection);

    /**
     * Used for metadata upgrade
     */
    void upgradePrivilegeCollection(
            PrivilegeCollectionV2 info, short pluginId, short metaVersion) throws PrivilegeException;
}
