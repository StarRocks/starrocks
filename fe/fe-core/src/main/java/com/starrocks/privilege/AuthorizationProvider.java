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

    boolean validateGrant(
            short type,
            Action want,
            PEntryObject object);

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

    void upgradePrivilegeCollection(
            PrivilegeCollection info, short pluginId, short metaVersion) throws PrivilegeException;
}
