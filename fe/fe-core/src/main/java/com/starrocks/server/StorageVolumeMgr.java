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

package com.starrocks.server;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.util.LockCloseable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.DdlException;
import com.starrocks.common.InvalidConfException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.persist.DropStorageVolumeLog;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.SetDefaultStorageVolumeLog;
import com.starrocks.persist.TableStorageInfos;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.sql.ast.AlterStorageVolumeStmt;
import com.starrocks.sql.ast.CreateStorageVolumeStmt;
import com.starrocks.sql.ast.DropStorageVolumeStmt;
import com.starrocks.sql.ast.SetDefaultStorageVolumeStmt;
import com.starrocks.storagevolume.CompositeStorageVolume;
import com.starrocks.storagevolume.StorageVolume;

import java.io.DataInput;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

// =====================================================================
// Composite SV Metadata Persistence Design
// =====================================================================
//
// Composite SV follows the SAME StarOS-only persistence model as regular SVs
// in shared-data mode (SharedDataStorageVolumeMgr):
//
// 1. SV DEFINITION (name, children, enabled, comment):
//    - Source of truth: StarOS FileStore (fsType = COMPOSITE_FS_TYPE_VALUE)
//    - FE does NOT cache SV definitions locally
//    - All reads go through StarOS RPC: getFileStore() / getFileStoreByName()
//    - This ensures FE failover does NOT lose Composite SV state
//
// 2. BINDING RELATIONSHIPS (table→SV, db→SV, default SV):
//    - Source of truth: FE Image + EditLog (same as regular SVs)
//    - Stored in storageVolumeToDbs / storageVolumeToTables
//    - Replayed on Follower via EditLog
//
// 3. CHILD REFERENCE CHECK (checkNotReferencedAsChildSv):
//    - Real-time query to StarOS listFileStore() (no local index)
//    - Only called during DROP/DISABLE operations (low frequency)
//
// Why NOT cache Composite SV definitions in FE:
// - Regular SVs don't cache either (SharedDataStorageVolumeMgr.getStorageVolume
//   always queries StarOS), so Composite SVs should not be special-cased
// - Eliminates dual-source-of-truth risk between FE in-memory cache and StarOS
// - Eliminates FE failover state loss (no EditLog replay needed for SV definitions)
// - Simplifies code: no cache invalidation, no replay methods, no rebuild logic
//
public abstract class StorageVolumeMgr implements Writable, GsonPostProcessable {
    private static final String ENABLED = "enabled";

    public static final String DEFAULT = "default";

    public static final String LOCAL = "local";

    public static final String BUILTIN_STORAGE_VOLUME = "builtin_storage_volume";

    private static final String S3 = "s3";

    private static final String AZBLOB = "azblob";

    private static final String ADLS2 = "adls2";

    private static final String GS = "gs";

    private static final String HDFS = "hdfs";

    public static final String COMPOSITE = "composite";

    @SerializedName("defaultSVId")
    protected String defaultStorageVolumeId = "";

    protected final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    // volume id to dbs
    @SerializedName("svToDbs")
    protected Map<String, Set<Long>> storageVolumeToDbs = new HashMap<>();

    // volume id to tables
    @SerializedName("svToTables")
    protected Map<String, Set<Long>> storageVolumeToTables = new HashMap<>();

    protected Map<Long, String> dbToStorageVolume = new HashMap<>();

    protected Map<Long, String> tableToStorageVolume = new HashMap<>();

    protected static final Set<String> PARAM_NAMES = new HashSet<>();

    static {
        Field[] fields = CloudConfigurationConstants.class.getFields();
        for (int i = 0; i < fields.length; ++i) {
            try {
                Object obj = CloudConfigurationConstants.class.newInstance();
                Object value = fields[i].get(obj);
                PARAM_NAMES.add((String) value);
            } catch (InstantiationException | IllegalAccessException e) {
                // do nothing
            }
        }
    }

    public String createStorageVolume(CreateStorageVolumeStmt stmt)
            throws AlreadyExistsException, DdlException {
        Map<String, String> params = new HashMap<>();
        Optional<Boolean> enabled = parseProperties(stmt.getProperties(), params);
        return createStorageVolume(stmt.getName(), stmt.getStorageVolumeType(), stmt.getStorageLocations(), params,
                enabled, stmt.getComment());
    }

    public String createStorageVolume(String name, String svType, List<String> locations, Map<String, String> params,
            Optional<Boolean> enabled, String comment)
            throws DdlException, AlreadyExistsException {
        // Route COMPOSITE type to composite-specific creation logic
        if (svType.equalsIgnoreCase(COMPOSITE)) {
            String childVolumesCsv = params.getOrDefault("child_volumes", "");
            if (childVolumesCsv.isEmpty()) {
                throw new DdlException(
                        "COMPOSITE storage volume requires property 'child_volumes' (comma-separated SV names)");
            }
            List<String> childNames = Arrays.asList(childVolumesCsv.split(","));
            return createCompositeStorageVolume(name, childNames, enabled.orElse(true), comment);
        }
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            validateParams(svType, params);
            validateLocations(svType, locations);
            if (exists(name)) {
                throw new AlreadyExistsException(String.format("Storage volume '%s' already exists", name));
            }
            return createInternalNoLock(name, svType, locations, params, enabled, comment);
        }
    }

    // =====================================================================
    // Composite SV management
    // =====================================================================

    /**
     * Create a COMPOSITE storage volume.
     *
     * <p><b>Persistence:</b> The Composite SV is persisted to StarOS FileStore via
     * {@link #createInternalNoLock}, following the same StarOS-only model as regular SVs.
     * No FE EditLog or local cache is used for SV definitions.
     *
     * @param name        composite SV name
     * @param childNames  ordered list of child SV names (will be resolved to IDs)
     * @param enabled     initial enabled state
     * @param comment     optional comment
     * @return composite SV id
     */
    public String createCompositeStorageVolume(String name, List<String> childNames,
                                               boolean enabled, String comment)
            throws DdlException, AlreadyExistsException {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            if (existsNoLock(name)) {
                throw new AlreadyExistsException(String.format("Storage volume '%s' already exists", name));
            }
            if (childNames.isEmpty()) {
                throw new DdlException("COMPOSITE storage volume must have at least one child SV");
            }
            // Resolve child names → IDs and validate
            List<String> childIds = resolveChildNamesToIds(childNames);
            Map<String, String> params = new HashMap<>();
            params.put(StorageVolume.COMPOSITE_CHILD_FS_KEYS, String.join(",", childIds));
            // Writes to StarOS FileStore — the sole source of truth for SV definitions
            return createInternalNoLock(name, COMPOSITE, Collections.emptyList(), params, Optional.of(enabled), comment);
        }
    }

    /**
     * Add a child SV to an existing Composite SV.
     * Increments membershipVersion so in-progress tablet creations see the old snapshot.
     * Updated state is written to StarOS FileStore (no local cache).
     */
    public void addChildToCompositeStorageVolume(String compositeName, String childName)
            throws DdlException {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            CompositeStorageVolume csv = getCompositeStorageVolumeByNameNoLock(compositeName);
            if (csv == null) {
                throw new DdlException("Composite storage volume '" + compositeName + "' not found");
            }
            StorageVolume child = getStorageVolumeByName(childName);
            if (child == null) {
                throw new DdlException("Child storage volume '" + childName + "' not found");
            }
            if (child.isComposite()) {
                throw new DdlException("Cannot add Composite SV '" + childName
                        + "' as child of '" + compositeName + "' (nesting not allowed)");
            }
            if (csv.getChildVolumeIds().contains(child.getId())) {
                throw new DdlException("Child SV '" + childName + "' is already in Composite SV '"
                        + compositeName + "'");
            }
            // Validate type consistency with existing children
            if (!csv.getChildVolumeIds().isEmpty()) {
                StorageVolume firstChild = getStorageVolume(csv.getChildVolumeIds().get(0));
                if (firstChild != null) {
                    String existingType = firstChild.getType().toUpperCase();
                    String newType = child.getType().toUpperCase();
                    if (!existingType.equals(newType)) {
                        throw new DdlException("Child SV '" + childName + "' type=" + newType
                                + " differs from existing children type=" + existingType
                                + " in Composite SV '" + compositeName + "'");
                    }
                }
            }
            CompositeStorageVolume updated = csv.withAddedChild(child.getId());
            // Persist to StarOS FileStore — the sole source of truth
            updateInternalNoLock(buildCompositeStorageVolumeNoLock(updated));
        }
    }

    /**
     * Remove a child SV from a Composite SV.
     *
     * <p><b>Safety guard (TC-RISK-02)</b>: If any tables are currently bound to this Composite SV,
     * existing tablets on the BE may physically reside on {@code childName}'s storage paths. Removing
     * the child from the Composite's membership would silently break the bucket→child mapping for
     * those tablets, causing potential data loss or path-resolution failures. To prevent this,
     * we refuse the operation when bound tables exist and require the user to unbind all tables
     * from the Composite SV first.
     *
     * <p>Precise per-shard verification via StarOS queries is left for a future enhancement once
     * an in-place child-swap workflow is designed.
     */
    public void removeChildFromCompositeStorageVolume(String compositeName, String childName)
            throws DdlException {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            CompositeStorageVolume csv = getCompositeStorageVolumeByNameNoLock(compositeName);
            if (csv == null) {
                throw new DdlException("Composite storage volume '" + compositeName + "' not found");
            }
            // Resolve child name → id (might be regular SV, need to look it up)
            StorageVolume child = getStorageVolumeByName(childName);
            if (child == null) {
                throw new DdlException("Storage volume '" + childName + "' not found");
            }
            String childId = child.getId();
            if (!csv.getChildVolumeIds().contains(childId)) {
                throw new DdlException("SV '" + childName + "' is not a child of Composite SV '"
                        + compositeName + "'");
            }

            // Safety guard: refuse REMOVE VOLUME when tables are bound to this Composite SV.
            // Existing tablets may physically reside on the child's storage paths; removing the
            // child from the membership would break bucket→child mapping for those tablets.
            Set<Long> boundTables = storageVolumeToTables.getOrDefault(csv.getId(), Collections.emptySet());
            Set<Long> boundDbs = storageVolumeToDbs.getOrDefault(csv.getId(), Collections.emptySet());
            if (!boundTables.isEmpty() || !boundDbs.isEmpty()) {
                throw new DdlException("[SV_COMPOSITE_HAS_BOUND_TABLES] Cannot remove child storage volume '"
                        + childName + "' from Composite SV '" + compositeName + "': "
                        + boundTables.size() + " table(s) and " + boundDbs.size()
                        + " database(s) are currently bound to this Composite SV. "
                        + "Unbind all tables and databases before modifying its child volumes.");
            }

            // Block removing child from the default Composite SV
            if (defaultStorageVolumeId.equals(csv.getId())) {
                throw new DdlException("Cannot remove child from default Composite SV '"
                        + compositeName + "'. Unset it as default first.");
            }

            CompositeStorageVolume updated = csv.withRemovedChild(childId);
            // Persist to StarOS FileStore — the sole source of truth
            updateInternalNoLock(buildCompositeStorageVolumeNoLock(updated));
        }
    }

    /** Returns the {@link CompositeStorageVolume} by name, or {@code null} if not found. */
    public CompositeStorageVolume getCompositeStorageVolumeByName(String name) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return getCompositeStorageVolumeByNameNoLock(name);
        }
    }

    /** Returns the {@link CompositeStorageVolume} by ID, or {@code null} if not found. */
    public CompositeStorageVolume getCompositeStorageVolume(String id) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return getCompositeStorageVolumeByIdNoLock(id);
        }
    }

    /**
     * Check that the given SV is NOT referenced as a child of any Composite SV.
     * Call this before DROP / DISABLE of a regular SV.
     *
     * <p><b>Implementation:</b> Real-time query to StarOS {@code listFileStore()} — no local
     * index is maintained. This is acceptable because DROP/DISABLE operations are infrequent.
     * This approach is consistent with the StarOS-only persistence model: FE never caches
     * Composite SV definitions, so there is no local reverse index to consult.
     */
    public void checkNotReferencedAsChildSv(String svId, String svName) throws DdlException {
        // Query all FileStores from StarOS, filter for COMPOSITE type, check child references
        List<String> referencingCompositeNames = new ArrayList<>();
        for (FileStoreInfo fsInfo : GlobalStateMgr.getCurrentState().getStarOSAgent().listFileStore()) {
            if (!StorageVolume.isCompositeFileStoreInfo(fsInfo)) {
                continue;
            }
            List<String> childIds = parseCompositeChildFsKeysFromFsInfo(fsInfo);
            if (childIds.contains(svId)) {
                referencingCompositeNames.add(fsInfo.getFsName());
            }
        }
        if (!referencingCompositeNames.isEmpty()) {
            throw new DdlException("[SV_CHILD_REFERENCED_BY_COMPOSITE] Cannot drop/disable storage volume '"
                    + svName + "': still referenced by Composite SVs: " + referencingCompositeNames);
        }
    }

    // ---- private helpers for Composite SV ----
    // All queries go directly to StarOS — no local cache. See class-level Javadoc.

    /**
     * Query StarOS by name. Returns null if not found or not a COMPOSITE type.
     */
    private CompositeStorageVolume getCompositeStorageVolumeByNameNoLock(String name) {
        try {
            FileStoreInfo fsInfo = GlobalStateMgr.getCurrentState().getStarOSAgent().getFileStoreByName(name);
            if (!StorageVolume.isCompositeFileStoreInfo(fsInfo)) {
                return null;
            }
            return new CompositeStorageVolume(fsInfo.getFsKey(), fsInfo.getFsName(),
                    parseCompositeChildFsKeysFromFsInfo(fsInfo), fsInfo.getEnabled(), fsInfo.getComment());
        } catch (DdlException e) {
            return null;
        }
    }

    /**
     * Query StarOS by id. Returns null if not found or not a COMPOSITE type.
     */
    protected CompositeStorageVolume getCompositeStorageVolumeByIdNoLock(String id) {
        try {
            FileStoreInfo fsInfo = GlobalStateMgr.getCurrentState().getStarOSAgent().getFileStore(id);
            if (!StorageVolume.isCompositeFileStoreInfo(fsInfo)) {
                return null;
            }
            return new CompositeStorageVolume(fsInfo.getFsKey(), fsInfo.getFsName(),
                    parseCompositeChildFsKeysFromFsInfo(fsInfo), fsInfo.getEnabled(), fsInfo.getComment());
        } catch (DdlException e) {
            return null;
        }
    }

    private StorageVolume buildCompositeStorageVolumeNoLock(CompositeStorageVolume csv) throws DdlException {
        Map<String, String> params = new HashMap<>();
        params.put(StorageVolume.COMPOSITE_CHILD_FS_KEYS, String.join(",", csv.getChildVolumeIds()));
        return new StorageVolume(csv.getId(), csv.getName(), COMPOSITE, Collections.emptyList(),
                params, csv.isEnabled(), csv.getComment());
    }

    /** Resolve a list of child SV names to IDs, validating constraints. */
    private List<String> resolveChildNamesToIds(List<String> childNames) throws DdlException {
        List<String> childIds = new ArrayList<>(childNames.size());
        StorageVolume.StorageVolumeType expectedType = null;
        for (String childName : childNames) {
            childName = childName.trim();
            StorageVolume child = getStorageVolumeByName(childName);
            if (child == null) {
                throw new DdlException("Child storage volume '" + childName + "' not found");
            }
            if (child.isComposite()) {
                throw new DdlException("Nested Composite SV is not allowed: '" + childName + "'");
            }
            StorageVolume.StorageVolumeType childType =
                    StorageVolume.StorageVolumeType.valueOf(child.getType().toUpperCase());
            if (expectedType == null) {
                expectedType = childType;
            } else if (expectedType != childType) {
                throw new DdlException("Child SV type mismatch: all child SVs must be the same type."
                        + " Expected " + expectedType + " but got " + childType
                        + " for SV '" + childName + "'");
            }
            childIds.add(child.getId());
        }
        return childIds;
    }

    private boolean existsNoLock(String svName) {
        if (getCompositeStorageVolumeByNameNoLock(svName) != null) {
            return true;
        }
        return getStorageVolumeByName(svName) != null;
    }

    private List<String> parseCompositeChildFsKeysFromFsInfo(FileStoreInfo fsInfo) {
        String csv = fsInfo.getPropertiesMap().getOrDefault(StorageVolume.COMPOSITE_CHILD_FS_KEYS, "");
        if (csv.isEmpty()) {
            return Collections.emptyList();
        }
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    public void removeStorageVolume(DropStorageVolumeStmt stmt) throws DdlException, MetaNotFoundException {
        removeStorageVolume(stmt.getName());
    }

    public void removeStorageVolume(String name) throws DdlException, MetaNotFoundException {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            // Handle Composite SV removal — query StarOS directly (no local cache)
            CompositeStorageVolume csv = getCompositeStorageVolumeByNameNoLock(name);
            if (csv != null) {
                Preconditions.checkState(!defaultStorageVolumeId.equals(csv.getId()),
                        "default storage volume can not be removed");
                // Block drop if Composite SV is still referenced by dbs or tables
                Set<Long> dbs = storageVolumeToDbs.getOrDefault(csv.getId(), new HashSet<>());
                Set<Long> tables = storageVolumeToTables.getOrDefault(csv.getId(), new HashSet<>());
                Preconditions.checkState(dbs.isEmpty() && tables.isEmpty(),
                        "Storage volume '%s' is referenced by dbs or tables, dbs: %s, tables: %s",
                        name, dbs.toString(), tables.toString());
                // Removes from StarOS FileStore — the sole source of truth
                removeInternalNoLock(buildCompositeStorageVolumeNoLock(csv));
                return;
            }

            // Regular SV: original logic + composite reference check
            StorageVolume sv = getStorageVolumeByName(name);
            if (sv == null) {
                throw new MetaNotFoundException(String.format("Storage volume '%s' does not exist", name));
            }
            String svName = GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().getAutomatedSnapshotSvName();
            if (svName != null && svName.equals(name)) {
                throw new DdlException(String.format("Snapshot enabled on storage volume '%s', drop volume failed.", name));
            }
            Preconditions.checkState(!defaultStorageVolumeId.equals(sv.getId()),
                    "default storage volume can not be removed");
            // V4: block drop if this SV is a child of any Composite SV
            checkNotReferencedAsChildSv(sv.getId(), name);
            Set<Long> dbs = storageVolumeToDbs.getOrDefault(sv.getId(), new HashSet<>());
            Set<Long> tables = storageVolumeToTables.getOrDefault(sv.getId(), new HashSet<>());
            if (name.equals(BUILTIN_STORAGE_VOLUME)) {
                List<List<Long>> bindings = getBindingsOfBuiltinStorageVolume();
                dbs.addAll(bindings.get(0));
                tables.addAll(bindings.get(1));
            }
            Preconditions.checkState(dbs.isEmpty() && tables.isEmpty(),
                    "Storage volume '%s' is referenced by dbs or tables, dbs: %s, tables: %s",
                    name, dbs.toString(), tables.toString());
            removeInternalNoLock(sv);
        }
    }

    public void updateStorageVolume(AlterStorageVolumeStmt stmt) throws DdlException, MetaNotFoundException {
        // Handle ADD VOLUME / REMOVE VOLUME for Composite SV
        if (stmt.isAddVolume()) {
            addChildToCompositeStorageVolume(stmt.getName(), stmt.getAddVolumeName());
            return;
        }
        if (stmt.isRemoveVolume()) {
            removeChildFromCompositeStorageVolume(stmt.getName(), stmt.getRemoveVolumeName());
            return;
        }
        // Regular property/comment modification
        updateStorageVolume(stmt.getName(), null, null, stmt.getProperties(), stmt.getComment());
    }

    public void updateStorageVolume(String name, String svType, List<String> locations,
            Map<String, String> properties, String comment) throws DdlException, MetaNotFoundException {
        Map<String, String> params = new HashMap<>();
        Optional<Boolean> enabled = parseProperties(properties, params);
        updateStorageVolume(name, svType, locations, params, enabled, comment);
    }

    public void updateStorageVolume(String name, String svType, List<String> locations,
            Map<String, String> params, Optional<Boolean> enabled, String comment) throws DdlException, MetaNotFoundException {
        List<String> immutableProperties = Lists.newArrayList(CloudConfigurationConstants.AWS_S3_NUM_PARTITIONED_PREFIX,
                CloudConfigurationConstants.AWS_S3_ENABLE_PARTITIONED_PREFIX);
        for (String param : immutableProperties) {
            if (params.containsKey(param)) {
                throw new DdlException(String.format("Storage volume property '%s' is immutable!", param));
            }
        }
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            // Composite SVs have no cloud config — detect and route to dedicated update path.
            CompositeStorageVolume csv = getCompositeStorageVolumeByNameNoLock(name);
            if (csv != null) {
                updateCompositeStorageVolumeNoLock(csv, params, enabled, comment);
                return;
            }

            StorageVolume sv = getStorageVolumeByName(name);
            if (sv == null) {
                throw new MetaNotFoundException(String.format("Storage volume '%s' does not exist", name));
            }
            StorageVolume copied = new StorageVolume(sv);
            validateParams(copied.getType(), params);

            if (enabled.isPresent()) {
                boolean enabledValue = enabled.get();
                if (!enabledValue) {
                    Preconditions.checkState(!copied.getId().equals(defaultStorageVolumeId),
                            "Default volume can not be disabled");
                    // [BUG-1 FIX] Block disabling a regular SV that is referenced as a child
                    // of any Composite SV. Disabling such an SV would cause Fail-Fast during
                    // new partition creation on all bound Composite SV tables.
                    checkNotReferencedAsChildSv(sv.getId(), name);
                }
                copied.setEnabled(enabledValue);
            }

            if (!Strings.isNullOrEmpty(svType)) {
                copied.setType(svType);
            }

            if (locations != null) {
                copied.setLocations(locations);
            }

            if (!Strings.isNullOrEmpty(comment)) {
                copied.setComment(comment);
            }

            if (!params.isEmpty()) {
                copied.setCloudConfiguration(params);
            }

            updateInternalNoLock(copied);
        }
    }

    public void replaceStorageVolume(String name, String svType, List<String> locations,
            Map<String, String> properties, String comment) throws DdlException {
        Map<String, String> params = new HashMap<>();
        Optional<Boolean> enabled = parseProperties(properties, params);
        validateParams(svType, params);

        String locationChangedStorageVolumeId = null;
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            StorageVolume oldStorageVolume = getStorageVolumeByName(name);
            Preconditions.checkState(oldStorageVolume != null, "Storage volume '%s' does not exist", name);
            if (enabled.isPresent()) {
                if (!enabled.get()) {
                    Preconditions.checkState(!oldStorageVolume.getId().equals(defaultStorageVolumeId),
                            "Default volume can not be disabled");
                }
            }

            StorageVolume newStorageVolume = null;
            if (oldStorageVolume.getType().equalsIgnoreCase(svType)) {
                newStorageVolume = new StorageVolume(oldStorageVolume);
                if (enabled.isPresent()) {
                    newStorageVolume.setEnabled(enabled.get());
                }

                if (!Strings.isNullOrEmpty(svType)) {
                    newStorageVolume.setType(svType);
                }

                if (locations != null) {
                    if (!oldStorageVolume.getLocations().equals(locations)) {
                        validateLocations(svType, locations);
                        newStorageVolume.setLocations(locations);
                        locationChangedStorageVolumeId = newStorageVolume.getId();
                    }
                }

                if (!Strings.isNullOrEmpty(comment)) {
                    newStorageVolume.setComment(comment);
                }

                if (!params.isEmpty()) {
                    newStorageVolume.setCloudConfiguration(params);
                }
            } else {
                Preconditions.checkState(locations != null, "Location is null");
                validateLocations(svType, locations);
                newStorageVolume = new StorageVolume(oldStorageVolume.getId(), name, svType, locations, params,
                        enabled.orElse(oldStorageVolume.getEnabled()), comment);

                if (oldStorageVolume.getVTabletId() != -1) {
                    newStorageVolume.setVTabletId(oldStorageVolume.getVTabletId());
                }

                if (oldStorageVolume.getVTabletGroupId() != -1) {
                    newStorageVolume.setVTabletGroupId(oldStorageVolume.getVTabletGroupId());
                }

                locationChangedStorageVolumeId = newStorageVolume.getId();
            }

            replaceInternalNoLock(newStorageVolume);
        }

        if (locationChangedStorageVolumeId != null) {
            updateTableStorageInfo(locationChangedStorageVolumeId);
        }
    }

    public void updateStorageVolumeVTabletMapping(String name, long vTabletId, long vTabletGroupId)
            throws DdlException {
        throw new DdlException("Not implemented");
    }

    /**
     * Update path for Composite SVs (enabled/comment only, no cloud config).
     *
     * <p>Composite SVs have no cloud configuration of their own, so only {@code COMMENT} and
     * the {@code enabled} property are supported. Cloud configuration params are rejected.
     * Updated state is persisted to StarOS FileStore via {@link #updateInternalNoLock}.
     *
     * <p>Must be called while holding the write lock.
     */
    private void updateCompositeStorageVolumeNoLock(CompositeStorageVolume csv,
                                                     Map<String, String> params,
                                                     Optional<Boolean> enabled,
                                                     String comment) throws DdlException {
        if (!params.isEmpty()) {
            throw new DdlException(
                    "COMPOSITE storage volume does not support cloud configuration parameters. "
                            + "Only COMMENT and the 'enabled' property are supported.");
        }
        if (enabled.isPresent()) {
            boolean enabledValue = enabled.get();
            if (!enabledValue) {
                Preconditions.checkState(!defaultStorageVolumeId.equals(csv.getId()),
                        "Default storage volume can not be disabled");
            }
            csv.setEnabled(enabledValue);
        }
        if (!comment.isEmpty()) {
            csv.setComment(comment);
        }
        updateInternalNoLock(buildCompositeStorageVolumeNoLock(csv));
    }

    public void setDefaultStorageVolume(SetDefaultStorageVolumeStmt stmt) {
        setDefaultStorageVolume(stmt.getName());
    }

    /**
     * Sets the given storage volume (regular or Composite) as the cluster-wide default.
     *
     * <p>For Composite SVs, {@link SharedDataStorageVolumeMgr#getStorageVolumeByName} returns a
     * lightweight {@link StorageVolume#createCompositeWrapper wrapper} whose {@code id} is the
     * Composite SV's UUID. That UUID is persisted in the {@link SetDefaultStorageVolumeLog} and
     * restored on FE restart. Subsequent calls to {@link #getDefaultStorageVolume()} resolve the
     * Composite SV via {@link SharedDataStorageVolumeMgr#getStorageVolume(String)}, which queries
     * StarOS by id — no local cache needed.
     */
    public void setDefaultStorageVolume(String svName) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            StorageVolume sv = getStorageVolumeByName(svName);
            Preconditions.checkState(sv != null, "Storage volume '%s' does not exist", svName);
            Preconditions.checkState(sv.getEnabled(), "Storage volume '%s' is disabled", svName);
            SetDefaultStorageVolumeLog log = new SetDefaultStorageVolumeLog(sv.getId());
            GlobalStateMgr.getCurrentState().getEditLog()
                    .logSetDefaultStorageVolume(log, wal -> this.defaultStorageVolumeId = sv.getId());
        }
    }

    public String getDefaultStorageVolumeId() {
        return defaultStorageVolumeId;
    }

    public boolean exists(String svName) throws DdlException {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            if (getCompositeStorageVolumeByNameNoLock(svName) != null) {
                return true;
            }
            StorageVolume sv = getStorageVolumeByName(svName);
            return sv != null;
        }
    }

    private Optional<Boolean> parseProperties(Map<String, String> properties, Map<String, String> params) {
        params.putAll(properties);
        Optional<Boolean> enabled = Optional.empty();
        if (params.containsKey(ENABLED)) {
            enabled = Optional.of(Boolean.parseBoolean(params.get(ENABLED)));
            params.remove(ENABLED);
        }
        return enabled;
    }

    public String getStorageVolumeIdOfTable(long tableId) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return tableToStorageVolume.get(tableId);
        }
    }

    public String getStorageVolumeIdOfDb(long dbId) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return dbToStorageVolume.get(dbId);
        }
    }

    public String getStorageVolumeNameOfTable(long tableId) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            String svId = getStorageVolumeIdOfTable(tableId);
            // If sv id is null, the table is upgraded from old version.
            // Builtin storage volume will be returned.
            return svId != null ? getStorageVolume(svId).getName() : BUILTIN_STORAGE_VOLUME;
        }
    }

    public String getStorageVolumeNameOfDb(long dbId) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            String svId = getStorageVolumeIdOfDb(dbId);
            // If sv id is null, the db is upgraded from old version.
            // Builtin storage volume will be returned.
            return svId != null ? getStorageVolume(svId).getName() : BUILTIN_STORAGE_VOLUME;
        }
    }

    public StorageVolume getDefaultStorageVolume() {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return getStorageVolume(getDefaultStorageVolumeId());
        }
    }

    public String getStorageVolumeName(String svId) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            StorageVolume sv = getStorageVolume(svId);
            if (sv == null) {
                return "";
            }
            return getStorageVolume(svId).getName();
        }
    }

    public void replaySetDefaultStorageVolume(SetDefaultStorageVolumeLog log) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            defaultStorageVolumeId = log.getId();
        }
    }

    public void replayCreateStorageVolume(StorageVolume sv) {
    }

    public void replayUpdateStorageVolume(StorageVolume sv) {
    }

    public void replayDropStorageVolume(DropStorageVolumeLog log) {
    }

    public void replayUpdateTableStorageInfos(TableStorageInfos tableStorageInfos) {
        throw new RuntimeException("Not implemented");
    }

    /**
     * For a table bound to a Composite SV, resolve the child SV for the given partition via round-robin
     * and return a partition-scoped {@link FilePathInfo}. Returns {@code null} if the table is not
     * bound to a Composite SV.
     *
     * @param table               the lake table
     * @param dbId                database ID
     * @param partitionId         logical partition ID (used for round-robin child selection)
     * @param physicalPartitionId physical partition ID (used for file path construction)
     */
    @Nullable
    public FilePathInfo resolveCompositePartitionFilePathInfo(OlapTable table, long dbId,
                                                              long partitionId, long physicalPartitionId)
            throws DdlException {
        String svId = getStorageVolumeIdOfTable(table.getId());
        if (svId == null) {
            return null;
        }
        CompositeStorageVolume csv = getCompositeStorageVolume(svId);
        if (csv == null) {
            return null;
        }
        CompositeStorageVolume.ChildVolumesSnapshot snapshot = csv.resolveChildVolumesStrict(this);
        int childIndex = (int) (Math.abs(partitionId % snapshot.childVolumes.size()));
        StorageVolume selectedChild = snapshot.childVolumes.get(childIndex);
        return GlobalStateMgr.getCurrentState().getStarOSAgent().allocatePartitionFilePathInfoForChildSv(
                selectedChild.getId(), dbId, table.getId(), physicalPartitionId);
    }

    /**
     * Returns the correct partition {@link FilePathInfo} for creating shards, handling both
     * Composite SV (round-robin child selection) and regular SV (table-level default).
     */
    public FilePathInfo getPartitionFilePathInfo(OlapTable table, long dbId,
                                                  long partitionId, long physicalPartitionId)
            throws DdlException {
        FilePathInfo overridePath = resolveCompositePartitionFilePathInfo(table, dbId, partitionId, physicalPartitionId);
        return overridePath != null ? overridePath : table.getPartitionFilePathInfo(physicalPartitionId);
    }

    protected void validateParams(String svType, Map<String, String> params) throws DdlException {
        if (svType.equalsIgnoreCase(HDFS)) {
            return;
        }
        for (String key : params.keySet()) {
            if (!PARAM_NAMES.contains(key)) {
                throw new DdlException("Invalid properties " + key);
            }
        }

        // storage volume type specific checks
        if (!svType.equalsIgnoreCase(S3)) {
            // The following two properties can be only set when storage volume type is 'S3'
            List<String> s3Params = Lists.newArrayList(
                    CloudConfigurationConstants.AWS_S3_NUM_PARTITIONED_PREFIX,
                    CloudConfigurationConstants.AWS_S3_ENABLE_PARTITIONED_PREFIX);
            for (String param : s3Params) {
                if (params.containsKey(param)) {
                    throw new DdlException(
                            String.format("Invalid property '%s' for storage volume type '%s'", param, svType));
                }
            }
        }
        if (params.containsKey(CloudConfigurationConstants.AWS_S3_NUM_PARTITIONED_PREFIX)) {
            try {
                int value = Integer.parseInt(params.get(CloudConfigurationConstants.AWS_S3_NUM_PARTITIONED_PREFIX));
                if (value < 0) {
                    throw new DdlException(String.format(
                            "Invalid property value '%s' for property '%s', expecting a positive integer string.",
                            params.get(CloudConfigurationConstants.AWS_S3_NUM_PARTITIONED_PREFIX),
                            CloudConfigurationConstants.AWS_S3_NUM_PARTITIONED_PREFIX));
                }
            } catch (NumberFormatException e) {
                throw new DdlException(String.format(
                        "Invalid property value '%s' for property '%s', expecting a valid integer string.",
                        params.get(CloudConfigurationConstants.AWS_S3_NUM_PARTITIONED_PREFIX),
                        CloudConfigurationConstants.AWS_S3_NUM_PARTITIONED_PREFIX));
            }
        }
    }

    private void validateLocations(String svType, List<String> locations) throws DdlException {
        for (String location : locations) {
            try {
                URI uri = new URI(location);
                String scheme = uri.getScheme().toLowerCase();
                switch (svType.toLowerCase()) {
                    case S3:
                    case AZBLOB:
                    case ADLS2:
                    case GS:
                        if (!scheme.equalsIgnoreCase(svType)) {
                            throw new DdlException("Invalid location " + location);
                        }
                        break;
                    case HDFS:
                        String pattern = "[a-z][a-z0-9]*";
                        if (!Pattern.matches(pattern, scheme)) {
                            throw new DdlException("Invalid location " + location);
                        }
                        break;
                    default:
                        throw new DdlException("Unknown storage volume type: " + svType);
                }
            } catch (URISyntaxException e) {
                throw new DdlException(String.format("Invalid location %s, error: %s", location, e.getMessage()));
            }
        }
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.STORAGE_VOLUME_MGR, 1);
        writer.writeJson(this);
        writer.close();
    }

    public void load(SRMetaBlockReader reader)
            throws SRMetaBlockEOFException, IOException, SRMetaBlockException {
        StorageVolumeMgr data = reader.readJson(StorageVolumeMgr.class);
        this.storageVolumeToDbs = data.storageVolumeToDbs;
        this.storageVolumeToTables = data.storageVolumeToTables;
        this.defaultStorageVolumeId = data.defaultStorageVolumeId;
        this.dbToStorageVolume = data.dbToStorageVolume;
        this.tableToStorageVolume = data.tableToStorageVolume;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        // If user upgrades from 3.0 and the createTableInfo and createDbInfo is replayed,
        // the image will look like: "svToDbs":{"null":[12288,81921,49154,65541,20485]}, "svToTables":{"null":[12288]}
        // The mapping null to dbs and tables should be removed. These code can be removed when 3.0 is not supported.
        storageVolumeToDbs.remove("null");
        for (Map.Entry<String, Set<Long>> entry : storageVolumeToDbs.entrySet()) {
            for (Long dbId : entry.getValue()) {
                dbToStorageVolume.put(dbId, entry.getKey());
            }
        }
        storageVolumeToTables.remove("null");
        for (Map.Entry<String, Set<Long>> entry : storageVolumeToTables.entrySet()) {
            for (Long tableId : entry.getValue()) {
                tableToStorageVolume.put(tableId, entry.getKey());
            }
        }
    }

    public void load(DataInput in) throws IOException {
        String json = Text.readString(in);
        StorageVolumeMgr data = GsonUtils.GSON.fromJson(json, StorageVolumeMgr.class);
        this.storageVolumeToDbs = data.storageVolumeToDbs;
        this.storageVolumeToTables = data.storageVolumeToTables;
        this.defaultStorageVolumeId = data.defaultStorageVolumeId;
        this.dbToStorageVolume = data.dbToStorageVolume;
        this.tableToStorageVolume = data.tableToStorageVolume;
    }

    public abstract StorageVolume getStorageVolumeByName(String svName);

    public abstract StorageVolume getStorageVolume(String svId);

    public abstract List<String> listStorageVolumeNames() throws DdlException;

    protected abstract String createInternalNoLock(String name, String svType, List<String> locations,
            Map<String, String> params, Optional<Boolean> enabled, String comment)
            throws DdlException;

    protected abstract void updateInternalNoLock(StorageVolume sv) throws DdlException;

    protected abstract void replaceInternalNoLock(StorageVolume sv) throws DdlException;

    protected abstract void removeInternalNoLock(StorageVolume sv) throws DdlException;

    public abstract boolean bindDbToStorageVolume(String svName, long dbId) throws DdlException;

    public abstract void replayBindDbToStorageVolume(String svId, long dbId);

    public abstract void unbindDbToStorageVolume(long dbId);

    public abstract boolean bindTableToStorageVolume(String svName, long dbId, long tableId) throws DdlException;

    public abstract void replayBindTableToStorageVolume(String svId, long tableId);

    public abstract void unbindTableToStorageVolume(long tableId);

    public abstract String createBuiltinStorageVolume() throws DdlException, AlreadyExistsException;

    public abstract void validateStorageVolumeConfig() throws InvalidConfException;

    protected abstract List<List<Long>> getBindingsOfBuiltinStorageVolume();

    protected abstract void updateTableStorageInfo(String storageVolumeId) throws DdlException;

    public abstract long getOrCreateVirtualTabletId(String storageVolumeName, String srcServiceId)
            throws MetaNotFoundException;

    public abstract boolean hasStorageVolumeBindAsVirtualGroup(long shardGroupId);
}
