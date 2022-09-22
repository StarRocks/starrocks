// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import com.starrocks.server.GlobalStateMgr;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultAuthorizationProvider implements AuthorizationProvider {
    private static final short PLUGIN_ID = 1;
    private static final short PLUGIN_VERSION = 1;

    private static final Map<String, List<String>> VALID_MAP = new HashMap<>();

    private enum Type {
        TABLE;
    }

    static {
        VALID_MAP.put(Type.TABLE.toString(), Arrays.asList("SELECT"));
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
    public Map<String, List<String>> getValidPrivilegeTypeToActions() {
        return VALID_MAP;
    }

    @Override
    public PEntryObject generateObject(String typeStr, List<String> objectTokens, GlobalStateMgr mgr) throws PrivilegeException {
        Type type = Type.valueOf(typeStr);
        switch (type) {
            case TABLE:
                return TablePEntryObject.generate(mgr, objectTokens);

            default:
                throw new PrivilegeException("unexpected type " + typeStr);
        }
    }

    @Override
    public boolean validateGrant(short type, Action want, PEntryObject object) {
        return true;
    }

    @Override
    public boolean check(short type, Action want, PEntryObject object, PrivilegeCollection currentPrivilegeCollection) {
        return currentPrivilegeCollection.check(type, want, object);
    }

    @Override
    public boolean checkAnyObject(short type, Action want, PrivilegeCollection currentPrivilegeCollection) {
        return currentPrivilegeCollection.checkAnyObject(type, want);
    }

    @Override
    public boolean hasType(short type, PrivilegeCollection currentPrivilegeCollection) {
        return currentPrivilegeCollection.hasType(type);
    }

    @Override
    public boolean allowGrant(short type, Action want, PEntryObject object,
                              PrivilegeCollection currentPrivilegeCollection) {
        return currentPrivilegeCollection.allowGrant(type, want, object);
    }

    @Override
    public boolean upgradePrivilegeCollection(PrivilegeCollection info, short pluginId, short metaVersion) {
        return false;
    }
}
