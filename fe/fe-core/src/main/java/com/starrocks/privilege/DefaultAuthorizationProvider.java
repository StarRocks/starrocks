// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import com.starrocks.server.GlobalStateMgr;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultAuthorizationProvider implements AuthorizationProvider {
    private static final short PLUGIN_ID = 1;
    private static final short PLUGIN_VERSION = 1;

    private static final Map<String, List<String>> VALID_MAP = new HashMap<>();

    // support only all validate actions in table level for now
    static {
        VALID_MAP.put(PrivilegeTypes.TABLE.toString(), PrivilegeTypes.TABLE.getValidActions());
        VALID_MAP.put(PrivilegeTypes.DATABASE.toString(), PrivilegeTypes.DATABASE.getValidActions());
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
        PrivilegeTypes type = PrivilegeTypes.valueOf(typeStr);
        switch (type) {
            case TABLE:
                return TablePEntryObject.generate(mgr, objectTokens);

            case DATABASE:
                return DbPEntryObject.generate(mgr, objectTokens);

            default:
                throw new PrivilegeException("unexpected type " + typeStr);
        }
    }

    @Override
    public void validateGrant(short type, ActionSet wantSet, PEntryObject object) {
        // ADMIN/GRANT is not allowed on system type. TBD
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
    public void upgradePrivilegeCollection(PrivilegeCollection info, short pluginId, short metaVersion)
            throws PrivilegeException {
        if (pluginId != PLUGIN_ID && metaVersion != PLUGIN_VERSION) {
            throw new PrivilegeException(String.format(
                    "unexpected privilege collection %s; plugin id expect %d actual %d; version expect %d actual %d",
                    info.toString(), PLUGIN_ID, pluginId, PLUGIN_VERSION, metaVersion));
        }
    }
}
