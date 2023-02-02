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

import com.google.gson.annotations.SerializedName;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class PrivilegeCollection {
    private static final Logger LOG = LogManager.getLogger(PrivilegeCollection.class);

    @SerializedName("m")
    protected Map<Short, List<PrivilegeEntry>> typeToPrivilegeEntryList = new HashMap<>();

    public static class PrivilegeEntry implements Comparable<PrivilegeEntry> {
        @SerializedName(value = "a")
        protected ActionSet actionSet;
        @SerializedName(value = "o")
        protected PEntryObject object;
        @SerializedName(value = "g")
        protected boolean isGrant;

        public PrivilegeEntry(ActionSet actionSet, PEntryObject object, boolean isGrant) {
            this.actionSet = actionSet;
            this.object = object;
            this.isGrant = isGrant;
        }

        public PrivilegeEntry(PrivilegeEntry other) {
            this.actionSet = new ActionSet(other.actionSet);
            if (other.object == null) {
                this.object = null;
            } else {
                this.object = other.object.clone();
            }
            this.isGrant = other.isGrant;
        }


        public ActionSet getActionSet() {
            return actionSet;
        }

        public PEntryObject getObject() {
            return object;
        }

        @Override
        public int compareTo(PrivilegeEntry o) {
            return this.object.compareTo(o.object);
        }
    }

    private boolean objectMatch(PEntryObject entryObject, PEntryObject other) {
        if (entryObject == null) {
            return other == null;
        } else {
            return entryObject.match(other);
        }
    }

    /**
     * find exact matching entry: object + isGrant
     */
    private PrivilegeEntry findEntry(List<PrivilegeEntry> privilegeEntryList, PEntryObject object, boolean isGrant) {
        if (object == null) {
            for (PrivilegeEntry privilegeEntry : privilegeEntryList) {
                if (privilegeEntry.object == null && isGrant == privilegeEntry.isGrant) {
                    return privilegeEntry;
                }
            }
        } else {
            for (PrivilegeEntry privilegeEntry : privilegeEntryList) {
                if (privilegeEntry.object != null
                        && object.equals(privilegeEntry.object)
                        && isGrant == privilegeEntry.isGrant) {
                    return privilegeEntry;
                }
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
            Collections.sort(privilegeEntryList);
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

    public void grant(short type, ActionSet actionSet, List<PEntryObject> objects, boolean isGrant)
            throws PrivilegeException {
        typeToPrivilegeEntryList.computeIfAbsent(type, k -> new ArrayList<>());
        List<PrivilegeEntry> privilegeEntryList = typeToPrivilegeEntryList.get(type);
        if (objects == null) {
            // objects can be null, we should adjust it to a list of one null object
            objects = new ArrayList<>();
            objects.add(null);
        }
        for (PEntryObject object : objects) {
            grantObjectToList(actionSet, object, isGrant, privilegeEntryList);
        }
    }

    private void grantObjectToList(
            ActionSet actionSet, PEntryObject object, boolean isGrant, List<PrivilegeEntry> privilegeEntryList) {
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
                addAction(privilegeEntryList, entry, actionSet, object, true);
            } else {
                // intend to grant without grant option, and there's already an entry that grant with grant option
                // we should check for each action, for those that's not in the existing entry
                // we should create a new entry or add to the matching one
                ActionSet remaining = oppositeEntry.actionSet.difference(actionSet);
                if (! remaining.isEmpty()) {
                    addAction(privilegeEntryList, entry, remaining, object, false);
                }
            }
        }
    }

    public void revoke(short type, ActionSet actionSet, List<PEntryObject> objects, boolean isGrant)
            throws PrivilegeException {
        List<PrivilegeEntry> privilegeEntryList = typeToPrivilegeEntryList.get(type);
        if (privilegeEntryList == null) {
            LOG.debug("revoke a non-existence type {}", type);
            return;
        }
        if (objects == null) {
            // objects can be null, we should adjust it to a list of one null object
            objects = new ArrayList<>();
            objects.add(null);
        }
        for (PEntryObject object : objects) {
            PrivilegeEntry entry = findEntry(privilegeEntryList, object, isGrant);
            if (entry != null) {
                removeAction(privilegeEntryList, entry, actionSet);
            }
            // some of the actions may not be granted
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
        List<PrivilegeEntry> privilegeEntryList = typeToPrivilegeEntryList.get(type);
        if (privilegeEntryList == null) {
            return false;
        }
        for (PrivilegeEntry privilegeEntry : privilegeEntryList) {
            if (objectMatch(object, privilegeEntry.object) && privilegeEntry.actionSet.contains(want)) {
                return true;
            }
            // still looking for the next entry, maybe object match but with/without grant option
        }
        return false;
    }

    private boolean searchObject(short type, PEntryObject object, Action want) {
        List<PrivilegeEntry> privilegeEntryList = typeToPrivilegeEntryList.get(type);
        if (privilegeEntryList == null) {
            return false;
        }
        for (PrivilegeEntry privilegeEntry : privilegeEntryList) {
            // 1. objectMatch(object, privilegeEntry.object):
            //    checking if db1.table1 exists for a user that's granted with `ALL tables in db1` will return true
            // 2. objectMatch(privilegeEntry.object, object):
            //    checking if any table in db1 exists for a user that's granted with `db1.table1` will return true
            if (objectMatch(object, privilegeEntry.object) || objectMatch(privilegeEntry.object, object)) {
                if (want == null || privilegeEntry.actionSet.contains(want)) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean searchAnyActionOnObject(short type, PEntryObject object) {
        return searchObject(type, object, null);
    }

    public boolean searchActionOnObject(short type, PEntryObject object, Action want) {
        return searchObject(type, object, want);
    }

    public boolean allowGrant(short type, ActionSet wantSet, List<PEntryObject> objects) {
        List<PrivilegeEntry> privilegeEntryList = typeToPrivilegeEntryList.get(type);
        if (privilegeEntryList == null) {
            return false;
        }
        List<PEntryObject> unCheckedObjects = new ArrayList<>(objects);
        for (PrivilegeEntry privilegeEntry : privilegeEntryList) {
            Iterator<PEntryObject> iterator = unCheckedObjects.iterator();
            while (iterator.hasNext()) {
                PEntryObject object = iterator.next();
                if (privilegeEntry.isGrant && objectMatch(object, privilegeEntry.object)) {
                    if (privilegeEntry.actionSet.contains(wantSet)) {
                        iterator.remove();
                        if (unCheckedObjects.isEmpty()) {
                            // all objects are verified
                            return true;
                        }
                    } else {
                        if (!privilegeEntry.object.isFuzzyMatching()) {
                            return false;
                        }
                    }
                }
            } // for object in unChecked objects
        } // for entry in privilegeEntryList

        return false; // cannot find all or some of the object in collection
    }

    public void removeInvalidObject(GlobalStateMgr globalStateMgr) {
        Iterator<Map.Entry<Short, List<PrivilegeEntry>>> listIter = typeToPrivilegeEntryList.entrySet().iterator();
        while (listIter.hasNext()) {
            List<PrivilegeEntry> list = listIter.next().getValue();
            Iterator<PrivilegeEntry> entryIterator = list.iterator();
            while (entryIterator.hasNext()) {
                PrivilegeEntry entry = entryIterator.next();
                if (entry.object != null && !entry.object.isFuzzyMatching() && !entry.object.validate(globalStateMgr)) {
                    String entryStr = GsonUtils.GSON.toJson(entry);
                    LOG.info("find invalid object, will remove the entry now: {}", entryStr);
                    entryIterator.remove();
                }
            }
            if (list.isEmpty()) {
                listIter.remove();
            }
        }
    }

    public void merge(PrivilegeCollection other) {
        for (Map.Entry<Short, List<PrivilegeEntry>> typeEntry : other.typeToPrivilegeEntryList.entrySet()) {
            short typeId = typeEntry.getKey();
            ArrayList<PrivilegeEntry> otherList = (ArrayList<PrivilegeEntry>) typeEntry.getValue();
            if (!typeToPrivilegeEntryList.containsKey(typeId)) {
                // deep copy here
                List<PrivilegeEntry> clonedList = new ArrayList<>();
                for (PrivilegeEntry entry : otherList) {
                    clonedList.add(new PrivilegeEntry(entry));
                }
                typeToPrivilegeEntryList.put(typeId, clonedList);
            } else {
                List<PrivilegeEntry> typeList = typeToPrivilegeEntryList.get(typeId);
                for (PrivilegeEntry entry : otherList) {
                    grantObjectToList(entry.actionSet, entry.object, entry.isGrant, typeList);
                } // for privilege entry in other.list
            }
        } // for typeId, privilegeEntryList in other
    }

    public boolean isEmpty() {
        return typeToPrivilegeEntryList.isEmpty();
    }

    public Map<Short, List<PrivilegeEntry>> getTypeToPrivilegeEntryList() {
        return typeToPrivilegeEntryList;
    }
}
