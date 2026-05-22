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

package com.starrocks.storagevolume;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.StorageVolumeMgr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * CompositeStorageVolume represents a logical aggregation of multiple ordinary storage volumes.
 * It enables cross-bucket tablet distribution for a single table.
 *
 * <p><b>Key design decisions:</b>
 * <ul>
 *   <li>Metadata is stored in StarOS FileStore(COMPOSITE).</li>
 *   <li>Children are referenced by ID (stable across rename).</li>
 *   <li>{@code membershipVersion} increments on every ADD/REMOVE for snapshot-based concurrency.</li>
 *   <li>Resolution is strict (Fail-Fast): any invalid child immediately throws {@link DdlException}.</li>
 * </ul>
 *
 * <p><b>Usage in partition creation:</b>
 * Resolve child SVs at partition-creation time, then distribute partitions across children
 * via round-robin (partitionId % N). All tablets within the same partition share one child SV path.
 */
public class CompositeStorageVolume implements Writable {

    @SerializedName("id")
    private final String id;

    @SerializedName("name")
    private String name;

    /** Child SV IDs (stable across rename). */
    @SerializedName("childIds")
    private List<String> childVolumeIds;

    /** Monotonically increasing; incremented on every ADD/REMOVE child operation. */
    @SerializedName("version")
    private long membershipVersion;

    @SerializedName("enabled")
    private boolean enabled;

    @SerializedName("comment")
    private String comment;

    /** Full constructor used by serialization and programmatic creation. */
    public CompositeStorageVolume(String id, String name, List<String> childVolumeIds,
                                  boolean enabled, String comment) {
        this.id = id;
        this.name = name;
        this.childVolumeIds = new ArrayList<>(childVolumeIds);
        this.membershipVersion = 0L;
        this.enabled = enabled;
        this.comment = (comment == null) ? "" : comment;
    }

    /** Factory: create a new composite SV with an auto-generated UUID. */
    public static CompositeStorageVolume create(String name, List<String> childVolumeIds,
                                                boolean enabled, String comment) {
        return new CompositeStorageVolume(
                UUID.randomUUID().toString(), name, childVolumeIds, enabled, comment);
    }

    /**
     * Strictly resolves child SVs from {@code mgr}.
     * Throws {@link DdlException} immediately on any invalid child (no silent skip).
     * The returned snapshot is immutable and safe to use across concurrent DDL operations.
     *
     * @throws DdlException if any child is missing, disabled, nested composite, or type-mismatched
     */
    public synchronized ChildVolumesSnapshot resolveChildVolumesStrict(StorageVolumeMgr mgr)
            throws DdlException {
        if (childVolumeIds.isEmpty()) {
            throw new DdlException("[SV_COMPOSITE_EMPTY_CHILDREN] Composite SV '"
                    + name + "' has no child volumes");
        }
        StorageVolume.StorageVolumeType expectedType = null;
        List<StorageVolume> result = new ArrayList<>(childVolumeIds.size());
        for (String childId : childVolumeIds) {
            StorageVolume child = mgr.getStorageVolume(childId);
            if (child == null) {
                throw new DdlException("[SV_COMPOSITE_CHILD_NOT_FOUND] Child SV id=" + childId
                        + " not found, referenced by Composite SV '" + name + "'");
            }
            if (child.isComposite()) {
                throw new DdlException("[SV_COMPOSITE_NESTED] Nested Composite SV is not allowed,"
                        + " child id=" + childId + " in Composite SV '" + name + "'");
            }
            if (!child.getEnabled()) {
                throw new DdlException("[SV_COMPOSITE_CHILD_DISABLED] Child SV '"
                        + child.getName() + "' (id=" + childId + ") is disabled"
                        + " in Composite SV '" + name + "'");
            }
            StorageVolume.StorageVolumeType childType =
                    StorageVolume.StorageVolumeType.valueOf(child.getType().toUpperCase());
            if (expectedType == null) {
                expectedType = childType;
            } else if (expectedType != childType) {
                throw new DdlException("[SV_COMPOSITE_CHILD_TYPE_MISMATCH] Child SV '"
                        + child.getName() + "' type=" + childType
                        + " differs from expected=" + expectedType
                        + " in Composite SV '" + name + "'");
            }
            result.add(child);
        }
        return new ChildVolumesSnapshot(Collections.unmodifiableList(result), membershipVersion);
    }

    /**
     * Returns a new {@link CompositeStorageVolume} with {@code childId} appended
     * and {@code membershipVersion} incremented. This object is unchanged.
     */
    public CompositeStorageVolume withAddedChild(String childId) {
        List<String> newIds = new ArrayList<>(childVolumeIds);
        newIds.add(childId);
        CompositeStorageVolume updated =
                new CompositeStorageVolume(id, name, newIds, enabled, comment);
        updated.membershipVersion = this.membershipVersion + 1;
        return updated;
    }

    /**
     * Returns a new {@link CompositeStorageVolume} with {@code childId} removed
     * and {@code membershipVersion} incremented. This object is unchanged.
     */
    public CompositeStorageVolume withRemovedChild(String childId) {
        List<String> newIds = new ArrayList<>(childVolumeIds);
        newIds.remove(childId);
        CompositeStorageVolume updated =
                new CompositeStorageVolume(id, name, newIds, enabled, comment);
        updated.membershipVersion = this.membershipVersion + 1;
        return updated;
    }

    // ---- Getters / Setters ----

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getChildVolumeIds() {
        return Collections.unmodifiableList(childVolumeIds);
    }

    public long getMembershipVersion() {
        return membershipVersion;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    // ---- Serialization ----

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static CompositeStorageVolume read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), CompositeStorageVolume.class);
    }

    // ---- Inner classes ----

    /**
     * Immutable snapshot of resolved child SVs taken at a specific {@code membershipVersion}.
     * Callers should retain this snapshot throughout the entire partition-creation operation
     * to ensure consistent bucket-to-child-SV mapping even if DDL modifies the Composite SV concurrently.
     */
    public static class ChildVolumesSnapshot {
        public final List<StorageVolume> childVolumes;
        public final long membershipVersion;

        public ChildVolumesSnapshot(List<StorageVolume> childVolumes, long membershipVersion) {
            this.childVolumes = childVolumes;
            this.membershipVersion = membershipVersion;
        }
    }
}
