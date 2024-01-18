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

import com.google.gson.annotations.SerializedName;
import com.starrocks.cloudnative.storagevolume.StorageVolume;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.MetaNotFoundException;

import java.util.List;
import java.util.Objects;

public class StorageVolumePEntryObject implements PEntryObject {
    @SerializedName(value = "id")
    private String id;

    protected StorageVolumePEntryObject(String id) {
        this.id = id;
    }

    public static PEntryObject generate(GlobalStateMgr mgr, List<String> tokens) throws PrivilegeException {
        if (tokens.size() != 1) {
            throw new PrivilegeException("invalid object tokens, should have one: " + tokens);
        }
        String name = tokens.get(0);
        if (name.equals("*")) {
            return new StorageVolumePEntryObject(PrivilegeBuiltinConstants.ALL_STORAGE_VOLUMES_ID);
        } else {
            StorageVolume sv = null;
            sv = mgr.getStorageVolumeMgr().getStorageVolumeByName(name);
            // TODO: Change it to MetaNotFoundException
            if (sv == null) {
                throw new PrivObjNotFoundException("cannot find storage volume: " + tokens.get(0));
            }
            return new StorageVolumePEntryObject(sv.getId());
        }
    }

    public String getId() {
        return id;
    }

    @Override
    public boolean match(Object obj) {
        if (!(obj instanceof StorageVolumePEntryObject)) {
            return false;
        }
        StorageVolumePEntryObject other = (StorageVolumePEntryObject) obj;
        if (other.id.equals(PrivilegeBuiltinConstants.ALL_STORAGE_VOLUMES_ID)) {
            return true;
        }
        return other.id.equals(id);
    }

    @Override
    public boolean isFuzzyMatching() {
        return id.equals(PrivilegeBuiltinConstants.ALL_STORAGE_VOLUMES_ID);
    }

    @Override
    public boolean validate(GlobalStateMgr globalStateMgr) {
        return globalStateMgr.getStorageVolumeMgr().getStorageVolume(id) != null;
    }

    @Override
    public int compareTo(PEntryObject obj) {
        if (!(obj instanceof StorageVolumePEntryObject)) {
            throw new ClassCastException("cannot cast " + obj.getClass().toString() + " to " + this.getClass());
        }
        StorageVolumePEntryObject o = (StorageVolumePEntryObject) obj;
        // other > all
        if (id.equals(PrivilegeBuiltinConstants.ALL_STORAGE_VOLUMES_ID)) {
            if (o.id.equals(PrivilegeBuiltinConstants.ALL_STORAGE_VOLUMES_ID)) {
                return 0;
            } else {
                return 1;
            }
        }
        if (o.id.equals(PrivilegeBuiltinConstants.ALL_STORAGE_VOLUMES_ID)) {
            return -1;
        }
        return id.compareTo(o.id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StorageVolumePEntryObject that = (StorageVolumePEntryObject) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public PEntryObject clone() {
        return new StorageVolumePEntryObject(id);
    }

    @Override
    public String toString() {
        if (id.equalsIgnoreCase(PrivilegeBuiltinConstants.ALL_STORAGE_VOLUMES_ID)) {
            return "ALL STORAGE VOLUMES";
        } else {
            String storageVolumeName = GlobalStateMgr.getCurrentState().getStorageVolumeMgr().getStorageVolumeName(id);
            if (storageVolumeName.isEmpty()) {
                throw new MetaNotFoundException("Can't find storage volume : " + id);
            }
            return storageVolumeName;
        }
    }
}
