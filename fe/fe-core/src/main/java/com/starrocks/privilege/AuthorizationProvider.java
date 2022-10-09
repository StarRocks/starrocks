// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

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

    /**
     * validate if grant is allowed
     * e.g. To forbid `NODE` privilege being granted, we should put some code here.
     */
    void validateGrant(
            short type,
            ActionSet wantSet,
            PEntryObject object) throws PrivilegeException;

    /**
     * check if certain action of certain type is allowed on certain object.
     * Developers can implement their own logic here.
     */
    boolean check(
            short type,
            Action want,
            PEntryObject object,
            PrivilegeCollection currentPrivilegeCollection);

    boolean checkAnyObject(
            short type,
            Action want,
            PrivilegeCollection currentPrivilegeCollection);

    boolean hasType(
            short type,
            PrivilegeCollection currentPrivilegeCollection);

    boolean allowGrant(
            short type,
            Action want,
            PEntryObject object,
            PrivilegeCollection currentPrivilegeCollection);

    /**
     * Used for metadata upgrade
     */
    void upgradePrivilegeCollection(
            PrivilegeCollection info, short pluginId, short metaVersion) throws PrivilegeException;
}
