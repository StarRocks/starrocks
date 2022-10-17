// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import com.starrocks.analysis.UserIdentity;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;
import java.util.Map;

public interface AuthorizationProvider {

    /**
     * return id & version
     */
    short getPluginId();
    short getPluginVersion();

    /**
     * validated type(string) -> validated action list(string)
     */
    Map<String, List<String>> getValidPrivilegeTypeToActions();

    /**
     * generate PEntryObject by tokenlist
     */
    PEntryObject generateObject(String type, List<String> objectTokens, GlobalStateMgr mgr) throws PrivilegeException;

    PEntryObject generateUserObject(String type, UserIdentity user, GlobalStateMgr mgr) throws PrivilegeException;

    /**
     * generate PEntryObject by ON/IN ALL statements
     * e.g. GRANT SELECT ON ALL TABLES IN DATABASE db
     * grant create_table on all databases to userx
     * ==> allTypeList: ["databases"], restrictType: null, restrictName: null
     * grant select on all tables in database db1 to userx
     * ==> allTypeList: ["tables"], restrictType: database, restrictName: db1
     * grant select on all tables in all databases to userx
     * ==> allTypeList: ["tables", "databases"], restrictType: null, restrictName: null
     **/
    PEntryObject generateObject(
            String typeStr, List<String> allTypes, String restrictType, String restrictName, GlobalStateMgr mgr)
            throws PrivilegeException;

    /**
     * validate if grant is allowed
     * e.g. To forbid `NODE` privilege being granted, we should put some code here.
     */
    void validateGrant(
            String type,
            List<String> actions,
            List<PEntryObject> objects) throws PrivilegeException;

    /**
     * check if certain action of certain type is allowed on certain object.
     * Developers can implement their own logic here.
     */
    boolean check(
            short type,
            Action want,
            PEntryObject object,
            PrivilegeCollection currentPrivilegeCollection);

    boolean checkAnyAction(
            short type,
            PEntryObject object,
            PrivilegeCollection currentPrivilegeCollection);

    boolean allowGrant(
            short type,
            ActionSet wants,
            List<PEntryObject> objects,
            PrivilegeCollection currentPrivilegeCollection);

    /**
     * Used for metadata upgrade
     */
    void upgradePrivilegeCollection(
            PrivilegeCollection info, short pluginId, short metaVersion) throws PrivilegeException;
}
