// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PrivilegeCollection {
    private static final Logger LOG = LogManager.getLogger(PrivilegeCollection.class);

    @SerializedName("m")
    protected Map<Short, List<PrivilegeEntry>> typeToPrivilegeEntryList = new HashMap<>();

    static class PrivilegeEntry {
        @SerializedName(value = "a")
        private ActionSet actionSet;
        @SerializedName(value = "o")
        private PEntryObject object;
        @SerializedName(value = "g")
        private boolean isGrant;

        public PrivilegeEntry(ActionSet actionSet, PEntryObject object, boolean isGrant) {
            this.actionSet = actionSet;
            this.object = object;
            this.isGrant = isGrant;
        }
    }

    /**
     * find matching entry: object + isGrant
     */
    private PrivilegeEntry findEntry(List<PrivilegeEntry> privilegeEntryList, PEntryObject object, boolean isGrant) {
        for (PrivilegeEntry privilegeEntry : privilegeEntryList) {
            if (privilegeEntry.object.keyMatch(object) && isGrant == privilegeEntry.isGrant) {
                return privilegeEntry;
            }
        }
        return null;
    }

    /**
     * add action to current entry or create a new one if not exists.
     */
    private void addAction(
            List<PrivilegeEntry> privilegeEntryList,
            PrivilegeEntry entry,
            ActionSet actionSet,
            PEntryObject object,
            boolean isGrant) {
        if (entry == null) {
            privilegeEntryList.add(new PrivilegeEntry(actionSet, object, isGrant));
        } else {
            entry.actionSet.add(actionSet);
        }
    }

    /**
     * remove action from a certain entry or even the whole entry if no other action left.
     */
    private void removeAction(List<PrivilegeEntry> privilegeEntryList, PrivilegeEntry entry, ActionSet actionSet) {
        entry.actionSet.remove(actionSet);
        if (entry.actionSet.isEmpty()) {
            privilegeEntryList.remove(entry);
        }
    }

    public void grant(short type, ActionSet actionSet, List<PEntryObject> objects, boolean isGrant) {
        typeToPrivilegeEntryList.computeIfAbsent(type, k -> new ArrayList<>());
        List<PrivilegeEntry> privilegeEntryList = typeToPrivilegeEntryList.get(type);
        for (PEntryObject object : objects) {
            PrivilegeEntry entry = findEntry(privilegeEntryList, object, isGrant);
            PrivilegeEntry oppositeEntry = findEntry(privilegeEntryList, object, !isGrant);
            if (oppositeEntry == null) {
                // intend to grant with grant option, and there's no matching entry that grant without grant option
                // or intend to grant without grant option, and there's no matching entry that grant with grant option
                // either way it's simpler
                addAction(privilegeEntryList, entry, actionSet, object, isGrant);
            } else {
                if (isGrant) {
                    // intend to grant with grant option, and there's already an entry that grant without grant option
                    // we should remove the entry and create a new one or added to the matching one
                    removeAction(privilegeEntryList, oppositeEntry, actionSet);
                    addAction(privilegeEntryList, entry, actionSet, object, isGrant);
                } else {
                    // intend to grant without grant option, and there's already an entry that grant with grant option
                    // we should check for each action, for those that's not in the existing entry
                    // we should create a new entry or added to the matching one
                    ActionSet remaining = oppositeEntry.actionSet.difference(actionSet);
                    if (! remaining.isEmpty()) {
                        addAction(privilegeEntryList, entry, remaining, object, isGrant);
                    }
                }
            }
        } // for object in objects
    }

    public void revoke(short type, ActionSet actionSet, List<PEntryObject> objects, boolean isGrant) {
        if (!typeToPrivilegeEntryList.containsKey(type)) {
            LOG.debug("revoke a non-existence type {}", type);
            return;
        }
        List<PrivilegeEntry> privilegeEntryList = typeToPrivilegeEntryList.get(type);
        for (PEntryObject object : objects) {
            PrivilegeEntry entry = findEntry(privilegeEntryList, object, isGrant);
            if (entry != null) {
                removeAction(privilegeEntryList, entry, actionSet);
            }
            // some of the actions may not granted
            entry = findEntry(privilegeEntryList, object, !isGrant);
            if (entry != null) {
                // 1. intend to revoke with grant option but already grant object without grant option
                // 2. intend to revoke without grant option but already grant object with grant option
                // either way, we should remove the action here
                removeAction(privilegeEntryList, entry, actionSet);
            }
        }
        if (privilegeEntryList.isEmpty()) {
            typeToPrivilegeEntryList.remove(type);
        }
    }

    public boolean check(short type, Action want, PEntryObject object) {
        if (!typeToPrivilegeEntryList.containsKey(type)) {
            return false;
        }
        List<PrivilegeEntry> privilegeEntryList = typeToPrivilegeEntryList.get(type);
        for (PrivilegeEntry privilegeEntry : privilegeEntryList) {
            if (privilegeEntry.object.keyMatch(object) && privilegeEntry.actionSet.contain(want)) {
                return true;
            }
        }
        return false;
    }

    public boolean checkAnyObject(short type, Action want) {
        if (!typeToPrivilegeEntryList.containsKey(type)) {
            return false;
        }
        List<PrivilegeEntry> privilegeEntryList = typeToPrivilegeEntryList.get(type);
        for (PrivilegeEntry privilegeEntry : privilegeEntryList) {
            if (privilegeEntry.actionSet.contain(want)) {
                return true;
            }
        }
        return false;
    }

    public boolean hasType(short type) {
        return typeToPrivilegeEntryList.containsKey(type);
    }

    public boolean allowGrant(short type, Action want, PEntryObject object) {
        if (!typeToPrivilegeEntryList.containsKey(type)) {
            return false;
        }
        List<PrivilegeEntry> privilegeEntryList = typeToPrivilegeEntryList.get(type);
        for (PrivilegeEntry privilegeEntry : privilegeEntryList) {
            if (privilegeEntry.isGrant
                    && privilegeEntry.object.keyMatch(object)
                    && privilegeEntry.actionSet.contain(want)) {
                return true;
            }
        }
        return false;
    }
}
