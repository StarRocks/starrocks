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

package com.starrocks.context;

import com.google.common.collect.ImmutableList;
import com.google.gson.annotations.SerializedName;
import com.starrocks.context.policy.CollectionTypePolicy;
import com.starrocks.epack.persist.EditLogEPack;
import com.starrocks.persist.ContextOpLog;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Control-plane metadata manager for the semantic-context module.
 *
 * <p>Holds small, low-frequency metadata objects (contextbases, collections, workspaces, retrieval
 * profiles) in memory and persists them via FE image / edit log. Large per-entity payloads live in
 * internal tables bootstrapped by {@link ContextMetaManager}, not here.
 *
 * <p>Milestone 1 ships a skeleton: create/drop on the four object classes and full image round-trip.
 * Edit-log replay for individual ops is wired in Milestone 2 alongside CRUD executors.
 */
public class ContextMgr {

    private static final Logger LOG = LogManager.getLogger(ContextMgr.class);

    public static final class ContextBaseMeta {
        @SerializedName("id")
        private final long id;
        @SerializedName("n")
        private final String name;
        @SerializedName("p")
        private final Map<String, String> properties;

        public ContextBaseMeta(long id, String name, Map<String, String> properties) {
            this.id = id;
            this.name = name;
            this.properties = properties == null ? Collections.emptyMap() : new LinkedHashMap<>(properties);
        }

        public long getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public Map<String, String> getProperties() {
            return Collections.unmodifiableMap(properties);
        }

        /**
         * Returns the user that created this contextbase, or {@code null} for bases created
         * before ownership stamping was wired (or by code paths that don't carry a session
         * identity, e.g. image-loaded legacy state). Stored in {@code properties._owner_user}
         * so the field survives image roundtrip without a schema change.
         */
        public String getOwner() {
            return properties.get("_owner_user");
        }
    }

    public static final class CollectionMeta {
        @SerializedName("id")
        private final long id;
        @SerializedName("cb")
        private final long contextBaseId;
        @SerializedName("n")
        private final String name;
        @SerializedName("t")
        private final String collectionType;
        @SerializedName("p")
        private final Map<String, String> properties;

        public CollectionMeta(long id, long contextBaseId, String name, String collectionType,
                              Map<String, String> properties) {
            this.id = id;
            this.contextBaseId = contextBaseId;
            this.name = name;
            this.collectionType = collectionType;
            this.properties = properties == null ? Collections.emptyMap() : new LinkedHashMap<>(properties);
        }

        public long getId() {
            return id;
        }

        public long getContextBaseId() {
            return contextBaseId;
        }

        public String getName() {
            return name;
        }

        public String getCollectionType() {
            return collectionType;
        }

        public Map<String, String> getProperties() {
            return Collections.unmodifiableMap(properties);
        }
    }

    public static final class WorkspaceMeta {
        @SerializedName("id")
        private final long id;
        @SerializedName("cl")
        private final long collectionId;
        @SerializedName("n")
        private final String name;
        @SerializedName("p")
        private final Map<String, String> properties;

        public WorkspaceMeta(long id, long collectionId, String name, Map<String, String> properties) {
            this.id = id;
            this.collectionId = collectionId;
            this.name = name;
            this.properties = properties == null ? Collections.emptyMap() : new LinkedHashMap<>(properties);
        }

        public long getId() {
            return id;
        }

        public long getCollectionId() {
            return collectionId;
        }

        public String getName() {
            return name;
        }

        public Map<String, String> getProperties() {
            return Collections.unmodifiableMap(properties);
        }
    }

    public static final class RetrievalProfileMeta {
        @SerializedName("id")
        private final long id;
        @SerializedName("n")
        private final String name;
        @SerializedName("p")
        private final Map<String, String> properties;

        public RetrievalProfileMeta(long id, String name, Map<String, String> properties) {
            this.id = id;
            this.name = name;
            this.properties = properties == null ? Collections.emptyMap() : new LinkedHashMap<>(properties);
        }

        public long getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public Map<String, String> getProperties() {
            return Collections.unmodifiableMap(properties);
        }
    }

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final Map<String, ContextBaseMeta> contextBases = new LinkedHashMap<>();
    // Reverse index keyed by id, kept in sync with contextBases under writeLock. Lets
    // getContextBaseById() return in O(1) instead of scanning every entry on the by-name map.
    // The REST list flows (filterVisibleCollections / filterVisibleWorkspaces) call this once per
    // returned row, so the previous linear scan made list responses O(M·N).
    private final Map<Long, ContextBaseMeta> contextBasesById = new java.util.HashMap<>();
    private final Map<String, CollectionMeta> collections = new LinkedHashMap<>();
    private final Map<Long, CollectionMeta> collectionsById = new java.util.HashMap<>();
    private final Map<String, WorkspaceMeta> workspaces = new LinkedHashMap<>();
    private final Map<String, RetrievalProfileMeta> retrievalProfiles = new LinkedHashMap<>();

    public long createContextBase(String name, Map<String, String> properties, boolean ifNotExists) {
        lock.writeLock().lock();
        try {
            if (contextBases.containsKey(name)) {
                if (ifNotExists) {
                    return contextBases.get(name).getId();
                }
                throw new IllegalStateException("contextbase already exists: " + name);
            }
            long id = GlobalStateMgr.getCurrentState().getNextId();
            ContextBaseMeta meta = new ContextBaseMeta(id, name, properties);
            // EditLog write goes first. If the journal write throws (BDBJE failure, disk full,
            // leader transition mid-flush), we must NOT have already inserted the row into the
            // in-memory map — otherwise a subsequent image dump would persist a base that no
            // follower ever saw, and the leader/follower metadata maps would silently diverge.
            // Letting the EditLog exception propagate without touching the map keeps the failure
            // visible to the caller and the cluster state coherent.
            ((EditLogEPack) GlobalStateMgr.getCurrentState().getEditLog())
                    .logCreateContextBase(ContextOpLog.forContextBase(id, name, properties),
                            wal -> putContextBaseLocked(meta));
            LOG.info("create contextbase {} id={}", name, id);
            return id;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Merge {@code newProperties} into the existing contextbase's property map and persist via the
     * {@code OP_ALTER_CONTEXTBASE} edit-log entry. Returns the merged property snapshot for
     * convenience.
     *
     * <p>Semantics: {@code ifExists} controls the "missing" case — when true and the base doesn't
     * exist, this is a no-op; when false, an {@link IllegalStateException} is raised. Property
     * keys present in {@code newProperties} overwrite the existing values; keys absent from
     * {@code newProperties} are preserved.
     */
    public Map<String, String> alterContextBase(String name, Map<String, String> newProperties,
                                                boolean ifExists) {
        lock.writeLock().lock();
        try {
            ContextBaseMeta prev = contextBases.get(name);
            if (prev == null) {
                if (ifExists) {
                    return Collections.emptyMap();
                }
                throw new IllegalStateException("contextbase not found: " + name);
            }
            Map<String, String> merged = new LinkedHashMap<>(prev.getProperties());
            if (newProperties != null) {
                merged.putAll(newProperties);
            }
            ContextBaseMeta updated = new ContextBaseMeta(prev.getId(), prev.getName(), merged);
            // EditLog before mutation (see createContextBase rationale).
            ((EditLogEPack) GlobalStateMgr.getCurrentState().getEditLog())
                    .logAlterContextBase(ContextOpLog.forContextBase(prev.getId(), name, merged),
                            wal -> putContextBaseLocked(updated));
            LOG.info("alter contextbase {} id={} merged_keys={}", name, prev.getId(),
                    newProperties == null ? 0 : newProperties.size());
            return merged;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean dropContextBase(String name, boolean ifExists) {
        lock.writeLock().lock();
        try {
            ContextBaseMeta existing = contextBases.get(name);
            if (existing == null) {
                if (!ifExists) {
                    throw new IllegalStateException("contextbase not found: " + name);
                }
                return false;
            }
            // EditLog before mutation. Previously the in-memory remove (plus cascade) happened
            // first, so a journal-write failure would leave the leader with an emptied map while
            // the persisted state still carried the base — followers replaying from the journal
            // would never see the drop, and the next image dump would race the failure: if it
            // ran before the operator retried, the drop would silently take effect with no
            // journal record. Persist first, then mutate.
            ((EditLogEPack) GlobalStateMgr.getCurrentState().getEditLog())
                    .logDropContextBase(ContextOpLog.forName(name), wal -> {
                        removeContextBaseLocked(name);
                        // Cascade: every collection under `<name>.*` and every workspace under
                        // `<name>.*.*` becomes orphaned the moment the contextbase goes away. Removing
                        // them in the same write-lock section keeps the in-memory metadata internally
                        // consistent and matches what `replayDropContextBase` does on the followers.
                        cascadeRemoveCollectionsLocked(name);
                        cascadeRemoveWorkspacesLocked(name);
                    });
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Rename a contextbase in place. This is a metadata-only rekey: the base keeps its numeric
     * {@code id}, so all physical data (rows in {@code __internal_context} are keyed by
     * {@code contextbase_id}) and all privileges ({@link
     * com.starrocks.authorization.ContextBasePEntryObject} stores the id) survive untouched. Only
     * the in-memory name maps and their name-derived collection / workspace keys are re-keyed.
     *
     * @throws IllegalStateException if {@code oldName} does not exist or {@code newName} is taken.
     */
    public void renameContextBase(String oldName, String newName) {
        lock.writeLock().lock();
        try {
            ContextBaseMeta existing = contextBases.get(oldName);
            if (existing == null) {
                throw new IllegalStateException("contextbase not found: " + oldName);
            }
            if (oldName.equals(newName)) {
                throw new IllegalStateException("new contextbase name is identical to the old one: " + newName);
            }
            if (contextBases.containsKey(newName)) {
                throw new IllegalStateException("contextbase already exists: " + newName);
            }
            // EditLog before mutation (see createContextBase rationale). The payload carries the
            // stable id plus old/new names so followers apply the exact same rekey.
            ((EditLogEPack) GlobalStateMgr.getCurrentState().getEditLog())
                    .logRenameContextBase(ContextOpLog.forRename(existing.getId(), oldName, newName),
                            wal -> applyRenameContextBaseLocked(oldName, newName, existing));
            LOG.info("rename contextbase {} -> {} id={}", oldName, newName, existing.getId());
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void replayRenameContextBase(ContextOpLog log) {
        lock.writeLock().lock();
        try {
            // Rename payload: name = old name, qualifiedName = new name (see ContextOpLog.forRename).
            String oldName = log.getName();
            String newName = log.getQualifiedName();
            ContextBaseMeta existing = contextBases.get(oldName);
            if (existing == null) {
                LOG.warn("replayRenameContextBase missing {}", oldName);
                return;
            }
            applyRenameContextBaseLocked(oldName, newName, existing);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Shared rekey used by both the leader write path and follower replay. Caller holds the write
     * lock. Re-keys the contextbase entry itself (new name, same id + properties) plus every
     * collection ({@code <cb>.<collection>}) and workspace ({@code <cb>.<collection>.<workspace>})
     * whose map key is prefixed by the old name. Collection metas store only the plain collection
     * name, so their meta objects are reused as-is; workspace metas store the FULL qualified name,
     * so they are rebuilt with the new prefix.
     */
    private void applyRenameContextBaseLocked(String oldName, String newName, ContextBaseMeta existing) {
        removeContextBaseLocked(oldName);
        putContextBaseLocked(new ContextBaseMeta(existing.getId(), newName, existing.getProperties()));
        rekeyCollectionsLocked(oldName, newName);
        rekeyWorkspacesLocked(oldName, newName);
    }

    private void rekeyCollectionsLocked(String oldName, String newName) {
        String oldPrefix = oldName + ".";
        java.util.List<Map.Entry<String, CollectionMeta>> moved = new java.util.ArrayList<>();
        java.util.Iterator<Map.Entry<String, CollectionMeta>> it = collections.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, CollectionMeta> e = it.next();
            if (e.getKey().startsWith(oldPrefix)) {
                moved.add(e);
                it.remove();
            }
        }
        for (Map.Entry<String, CollectionMeta> e : moved) {
            // Meta is unchanged (contextBaseId + plain collection name are both stable); only the
            // name-derived map key changes. collectionsById already points at the same meta.
            collections.put(newName + "." + e.getKey().substring(oldPrefix.length()), e.getValue());
        }
    }

    private void rekeyWorkspacesLocked(String oldName, String newName) {
        String oldPrefix = oldName + ".";
        java.util.List<Map.Entry<String, WorkspaceMeta>> moved = new java.util.ArrayList<>();
        java.util.Iterator<Map.Entry<String, WorkspaceMeta>> it = workspaces.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, WorkspaceMeta> e = it.next();
            if (e.getKey().startsWith(oldPrefix)) {
                moved.add(e);
                it.remove();
            }
        }
        for (Map.Entry<String, WorkspaceMeta> e : moved) {
            String newKey = newName + "." + e.getKey().substring(oldPrefix.length());
            WorkspaceMeta old = e.getValue();
            // WorkspaceMeta.name holds the FULL qualified name, so rebuild it with the new prefix
            // (id + collectionId + properties are preserved).
            workspaces.put(newKey, new WorkspaceMeta(old.getId(), old.getCollectionId(), newKey, old.getProperties()));
        }
    }

    private void cascadeRemoveCollectionsLocked(String contextBase) {
        String prefix = contextBase + ".";
        java.util.Iterator<Map.Entry<String, CollectionMeta>> it = collections.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, CollectionMeta> e = it.next();
            if (e.getKey().startsWith(prefix)) {
                collectionsById.remove(e.getValue().getId());
                it.remove();
            }
        }
    }

    /** Insert / replace a contextbase meta, keeping the by-id index in sync. Caller holds write lock. */
    private void putContextBaseLocked(ContextBaseMeta meta) {
        contextBases.put(meta.getName(), meta);
        contextBasesById.put(meta.getId(), meta);
    }

    /** Remove a contextbase meta by name, clearing the by-id index too. Caller holds write lock. */
    private ContextBaseMeta removeContextBaseLocked(String name) {
        ContextBaseMeta removed = contextBases.remove(name);
        if (removed != null) {
            contextBasesById.remove(removed.getId());
        }
        return removed;
    }

    /** Insert / replace a collection meta keyed by qualified name, keeping by-id index in sync. */
    private void putCollectionLocked(String qualifiedName, CollectionMeta meta) {
        collections.put(qualifiedName, meta);
        collectionsById.put(meta.getId(), meta);
    }

    /** Remove a collection meta, clearing the by-id index. */
    private CollectionMeta removeCollectionLocked(String qualifiedName) {
        CollectionMeta removed = collections.remove(qualifiedName);
        if (removed != null) {
            collectionsById.remove(removed.getId());
        }
        return removed;
    }

    private void cascadeRemoveWorkspacesLocked(String contextBase) {
        String prefix = contextBase + ".";
        java.util.Iterator<Map.Entry<String, WorkspaceMeta>> it = workspaces.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, WorkspaceMeta> e = it.next();
            if (e.getKey().startsWith(prefix)) {
                it.remove();
            }
        }
    }

    public void replayCreateContextBase(ContextOpLog log) {
        lock.writeLock().lock();
        try {
            ContextBaseMeta meta = new ContextBaseMeta(log.getId(), log.getName(), log.getProperties());
            putContextBaseLocked(meta);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void replayAlterContextBase(ContextOpLog log) {
        lock.writeLock().lock();
        try {
            ContextBaseMeta prev = contextBases.get(log.getName());
            if (prev == null) {
                LOG.warn("replayAlterContextBase missing {}", log.getName());
                return;
            }
            Map<String, String> merged = new LinkedHashMap<>(prev.getProperties());
            if (log.getProperties() != null) {
                merged.putAll(log.getProperties());
            }
            putContextBaseLocked(new ContextBaseMeta(prev.getId(), prev.getName(), merged));
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void replayDropContextBase(ContextOpLog log) {
        lock.writeLock().lock();
        try {
            ContextBaseMeta removed = removeContextBaseLocked(log.getName());
            if (removed != null) {
                // Followers must apply the same cascade as the leader to keep the in-memory
                // metadata consistent across the cluster. Otherwise a follower would still report
                // the dropped base's collections / workspaces via SHOW CONTEXT *.
                cascadeRemoveCollectionsLocked(log.getName());
                cascadeRemoveWorkspacesLocked(log.getName());
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public ContextBaseMeta getContextBase(String name) {
        lock.readLock().lock();
        try {
            return contextBases.get(name);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Reverse lookup by id. Used by privilege filters that have a {@code contextbase_id} column
     * value (e.g. on a collection meta) and need the canonical name to feed
     * {@link com.starrocks.http.rest.context.ContextRestAuth#canSeeContextBase}.
     */
    public ContextBaseMeta getContextBaseById(long id) {
        lock.readLock().lock();
        try {
            // O(1) via the by-id reverse index. The previous implementation linear-scanned
            // contextBases.values(); the REST list endpoints call this once per returned row, so
            // the linear scan turned a list of N contextbases × M collections into O(M·N).
            return contextBasesById.get(id);
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<ContextBaseMeta> listContextBases() {
        lock.readLock().lock();
        try {
            return ImmutableList.copyOf(contextBases.values());
        } finally {
            lock.readLock().unlock();
        }
    }

    public long createCollection(String contextBase, String name, String collectionType,
                                 Map<String, String> properties, boolean ifNotExists) {
        lock.writeLock().lock();
        try {
            ContextBaseMeta cb = contextBases.get(contextBase);
            if (cb == null) {
                throw new IllegalStateException("contextbase not found: " + contextBase);
            }
            String key = contextBase + "." + name;
            if (collections.containsKey(key)) {
                if (ifNotExists) {
                    return collections.get(key).getId();
                }
                throw new IllegalStateException("collection already exists: " + key);
            }
            String normalizedCollectionType = collectionType == null ? CollectionTypePolicy.TYPE_KNOWLEDGE
                    : collectionType.trim().toLowerCase(java.util.Locale.ROOT);
            if (!CollectionTypePolicy.isValidCollectionType(normalizedCollectionType)) {
                throw new IllegalStateException("unknown collection_type: " + collectionType);
            }
            long id = GlobalStateMgr.getCurrentState().getNextId();
            CollectionMeta meta = new CollectionMeta(id, cb.getId(), name, normalizedCollectionType, properties);
            ((EditLogEPack) GlobalStateMgr.getCurrentState().getEditLog())
                    .logCreateContextCollection(
                            ContextOpLog.forCollection(id, cb.getId(), key, normalizedCollectionType, properties),
                            wal -> putCollectionLocked(key, meta));
            LOG.info("create collection {} id={}", key, id);
            return id;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean dropCollection(String contextBase, String name, boolean ifExists) {
        lock.writeLock().lock();
        try {
            String key = contextBase + "." + name;
            CollectionMeta existing = collections.get(key);
            if (existing == null) {
                if (!ifExists) {
                    throw new IllegalStateException("collection not found: " + key);
                }
                return false;
            }
            // EditLog before mutation (same rationale as createContextBase). Then cascade-remove
            // any workspaces that lived under this collection; without the cascade those rows
            // would survive in-memory and be re-persisted into the next image as orphans whose
            // parent collection no longer exists. Image load only used to verify the parent
            // contextbase was present; the matching guard there has been tightened too.
            ((EditLogEPack) GlobalStateMgr.getCurrentState().getEditLog())
                    .logDropContextCollection(ContextOpLog.forQualifiedName(key), wal -> {
                        removeCollectionLocked(key);
                        cascadeRemoveWorkspacesUnderCollectionLocked(contextBase, name);
                    });
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Remove every workspace whose qualified name starts with {@code <contextBase>.<collection>.}.
     * Caller must hold the write lock.
     */
    private void cascadeRemoveWorkspacesUnderCollectionLocked(String contextBase, String collectionName) {
        String prefix = contextBase + "." + collectionName + ".";
        java.util.Iterator<Map.Entry<String, WorkspaceMeta>> it = workspaces.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, WorkspaceMeta> e = it.next();
            if (e.getKey().startsWith(prefix)) {
                it.remove();
            }
        }
    }

    public void replayCreateCollection(ContextOpLog log) {
        lock.writeLock().lock();
        try {
            String key = log.getName();
            int dot = key.indexOf('.');
            String plainName = dot < 0 ? key : key.substring(dot + 1);
            putCollectionLocked(key,
                    new CollectionMeta(log.getId(), log.getParentId(), plainName, log.getTypeTag(),
                            log.getProperties()));
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void replayDropCollection(ContextOpLog log) {
        lock.writeLock().lock();
        try {
            String qn = log.getQualifiedName();
            removeCollectionLocked(qn);
            // Followers must apply the same workspace cascade as the leader; otherwise a
            // follower promoted later would carry orphan workspaces forward and persist them
            // into its first image. The qualified name format is `<contextbase>.<collection>`,
            // so the workspace prefix is `<qn>.`.
            if (qn != null) {
                int dot = qn.indexOf('.');
                if (dot > 0) {
                    cascadeRemoveWorkspacesUnderCollectionLocked(
                            qn.substring(0, dot), qn.substring(dot + 1));
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public List<CollectionMeta> listCollections(String contextBase) {
        lock.readLock().lock();
        try {
            if (contextBase == null) {
                return ImmutableList.copyOf(collections.values());
            }
            String prefix = contextBase + ".";
            ImmutableList.Builder<CollectionMeta> b = ImmutableList.builder();
            for (Map.Entry<String, CollectionMeta> e : collections.entrySet()) {
                if (e.getKey().startsWith(prefix)) {
                    b.add(e.getValue());
                }
            }
            return b.build();
        } finally {
            lock.readLock().unlock();
        }
    }

    public CollectionMeta getCollection(String qualifiedName) {
        lock.readLock().lock();
        try {
            return collections.get(qualifiedName);
        } finally {
            lock.readLock().unlock();
        }
    }

    public CollectionMeta getCollection(String contextBase, String collectionName) {
        if (contextBase == null || collectionName == null) {
            return null;
        }
        return getCollection(contextBase + "." + collectionName);
    }

    public CollectionMeta getCollectionById(long id) {
        lock.readLock().lock();
        try {
            return collectionsById.get(id);
        } finally {
            lock.readLock().unlock();
        }
    }

    public WorkspaceMeta getWorkspace(String qualifiedName) {
        lock.readLock().lock();
        try {
            return workspaces.get(qualifiedName);
        } finally {
            lock.readLock().unlock();
        }
    }

    public CollectionMeta resolveWorkspaceCollection(WorkspaceMeta workspace) {
        if (workspace == null) {
            return null;
        }
        if (workspace.getCollectionId() > 0) {
            CollectionMeta byId = getCollectionById(workspace.getCollectionId());
            if (byId != null) {
                return byId;
            }
        }
        String qualifiedName = workspace.getName();
        int lastDot = qualifiedName == null ? -1 : qualifiedName.lastIndexOf('.');
        if (lastDot <= 0) {
            return null;
        }
        return getCollection(qualifiedName.substring(0, lastDot));
    }

    public long createWorkspace(String qualifiedName, long collectionId,
                                Map<String, String> properties, boolean ifNotExists) {
        lock.writeLock().lock();
        try {
            if (workspaces.containsKey(qualifiedName)) {
                if (ifNotExists) {
                    return workspaces.get(qualifiedName).getId();
                }
                throw new IllegalStateException("workspace already exists: " + qualifiedName);
            }
            // The parent collection must already exist and its id must match collectionId. A
            // workspace name is `<contextbase>.<collection>.<workspace>`, so its parent collection
            // key is the prefix up to the last dot. Without this check the leader would journal and
            // expose a workspace that load() later drops as an orphan (leader-vs-restart divergence).
            int lastDot = qualifiedName == null ? -1 : qualifiedName.lastIndexOf('.');
            String parentCollectionKey = lastDot > 0 ? qualifiedName.substring(0, lastDot) : null;
            CollectionMeta parentCollection = parentCollectionKey == null ? null : collections.get(parentCollectionKey);
            if (parentCollection == null || parentCollection.getId() != collectionId) {
                throw new IllegalStateException("workspace parent collection not found or does not match"
                        + " collectionId " + collectionId + " for: " + qualifiedName);
            }
            long id = GlobalStateMgr.getCurrentState().getNextId();
            // EditLog before mutation (see createContextBase rationale). A journal-write
            // failure must not leave the leader holding a workspace no follower ever sees.
            ((EditLogEPack) GlobalStateMgr.getCurrentState().getEditLog())
                    .logCreateContextWorkspace(
                            ContextOpLog.forWorkspace(id, collectionId, qualifiedName, properties),
                            wal -> workspaces.put(qualifiedName,
                                    new WorkspaceMeta(id, collectionId, qualifiedName, properties)));
            return id;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean dropWorkspace(String qualifiedName, boolean ifExists) {
        lock.writeLock().lock();
        try {
            WorkspaceMeta existing = workspaces.get(qualifiedName);
            if (existing == null) {
                if (!ifExists) {
                    throw new IllegalStateException("workspace not found: " + qualifiedName);
                }
                return false;
            }
            // EditLog before mutation (see dropContextBase rationale).
            ((EditLogEPack) GlobalStateMgr.getCurrentState().getEditLog())
                    .logDropContextWorkspace(ContextOpLog.forQualifiedName(qualifiedName),
                            wal -> workspaces.remove(qualifiedName));
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void replayCreateWorkspace(ContextOpLog log) {
        lock.writeLock().lock();
        try {
            workspaces.put(log.getQualifiedName(),
                    new WorkspaceMeta(log.getId(), log.getParentId(), log.getQualifiedName(), log.getProperties()));
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void replayDropWorkspace(ContextOpLog log) {
        lock.writeLock().lock();
        try {
            workspaces.remove(log.getQualifiedName());
        } finally {
            lock.writeLock().unlock();
        }
    }

    public List<WorkspaceMeta> listWorkspaces(String contextBase) {
        lock.readLock().lock();
        try {
            if (contextBase == null) {
                return ImmutableList.copyOf(workspaces.values());
            }
            String prefix = contextBase + ".";
            ImmutableList.Builder<WorkspaceMeta> b = ImmutableList.builder();
            for (Map.Entry<String, WorkspaceMeta> e : workspaces.entrySet()) {
                if (e.getKey().startsWith(prefix)) {
                    b.add(e.getValue());
                }
            }
            return b.build();
        } finally {
            lock.readLock().unlock();
        }
    }

    public long createRetrievalProfile(String name, Map<String, String> properties, boolean ifNotExists) {
        lock.writeLock().lock();
        try {
            if (retrievalProfiles.containsKey(name)) {
                if (ifNotExists) {
                    return retrievalProfiles.get(name).getId();
                }
                throw new IllegalStateException("retrieval profile already exists: " + name);
            }
            long id = GlobalStateMgr.getCurrentState().getNextId();
            // EditLog before mutation (see createContextBase rationale).
            ((EditLogEPack) GlobalStateMgr.getCurrentState().getEditLog())
                    .logCreateContextRetrievalProfile(
                            ContextOpLog.forRetrievalProfile(id, name, properties),
                            wal -> retrievalProfiles.put(name, new RetrievalProfileMeta(id, name, properties)));
            return id;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean dropRetrievalProfile(String name, boolean ifExists) {
        lock.writeLock().lock();
        try {
            RetrievalProfileMeta existing = retrievalProfiles.get(name);
            if (existing == null) {
                if (!ifExists) {
                    throw new IllegalStateException("retrieval profile not found: " + name);
                }
                return false;
            }
            // EditLog before mutation (see dropContextBase rationale).
            ((EditLogEPack) GlobalStateMgr.getCurrentState().getEditLog())
                    .logDropContextRetrievalProfile(ContextOpLog.forName(name),
                            wal -> retrievalProfiles.remove(name));
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void replayCreateRetrievalProfile(ContextOpLog log) {
        lock.writeLock().lock();
        try {
            retrievalProfiles.put(log.getName(),
                    new RetrievalProfileMeta(log.getId(), log.getName(), log.getProperties()));
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void replayDropRetrievalProfile(ContextOpLog log) {
        lock.writeLock().lock();
        try {
            retrievalProfiles.remove(log.getName());
        } finally {
            lock.writeLock().unlock();
        }
    }

    public RetrievalProfileMeta getRetrievalProfile(String name) {
        lock.readLock().lock();
        try {
            return retrievalProfiles.get(name);
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<RetrievalProfileMeta> listRetrievalProfiles() {
        lock.readLock().lock();
        try {
            return ImmutableList.copyOf(retrievalProfiles.values());
        } finally {
            lock.readLock().unlock();
        }
    }

    // ---------------------------------------- Image round-trip ------------------------------------

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        lock.readLock().lock();
        try {
            // One block per object kind: 1 contextbases + 1 collections + 1 workspaces + 1 profiles
            int blockCount = 1 + contextBases.size() + 1 + collections.size()
                    + 1 + workspaces.size() + 1 + retrievalProfiles.size();
            SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.CONTEXT_MGR, blockCount);
            writer.writeInt(contextBases.size());
            for (ContextBaseMeta m : contextBases.values()) {
                writer.writeJson(m);
            }
            writer.writeInt(collections.size());
            for (CollectionMeta m : collections.values()) {
                writer.writeJson(m);
            }
            writer.writeInt(workspaces.size());
            for (WorkspaceMeta m : workspaces.values()) {
                writer.writeJson(m);
            }
            writer.writeInt(retrievalProfiles.size());
            for (RetrievalProfileMeta m : retrievalProfiles.values()) {
                writer.writeJson(m);
            }
            writer.close();
        } finally {
            lock.readLock().unlock();
        }
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        lock.writeLock().lock();
        try {
            contextBases.clear();
            contextBasesById.clear();
            collections.clear();
            collectionsById.clear();
            workspaces.clear();
            retrievalProfiles.clear();

            int cbCount = reader.readInt();
            for (int i = 0; i < cbCount; i++) {
                ContextBaseMeta m = reader.readJson(ContextBaseMeta.class);
                putContextBaseLocked(m);
            }
            int colCount = reader.readInt();
            for (int i = 0; i < colCount; i++) {
                CollectionMeta m = reader.readJson(CollectionMeta.class);
                String cbName = findContextBaseName(m.getContextBaseId());
                if (cbName == null) {
                    LOG.warn("orphan collection {} for contextbase {}", m.getName(), m.getContextBaseId());
                    continue;
                }
                putCollectionLocked(cbName + "." + m.getName(), m);
            }
            int wsCount = reader.readInt();
            int orphanWorkspaces = 0;
            for (int i = 0; i < wsCount; i++) {
                WorkspaceMeta m = reader.readJson(WorkspaceMeta.class);
                // A workspace's qualified name is `<contextbase>.<collection>.<workspace>`.
                // Both the parent contextbase AND the parent collection must be present in the
                // freshly-loaded maps; otherwise the workspace is dangling and should not be
                // admitted. The previous check only validated the contextbase prefix, which let
                // a workspace survive a DROP COLLECTION across an image load+replay cycle.
                String qn = m.getName();
                int firstDot = qn == null ? -1 : qn.indexOf('.');
                int lastDot = qn == null ? -1 : qn.lastIndexOf('.');
                String cbName = firstDot > 0 ? qn.substring(0, firstDot) : null;
                String collectionQn = (firstDot > 0 && lastDot > firstDot) ? qn.substring(0, lastDot) : null;
                if (cbName == null || !contextBases.containsKey(cbName)) {
                    LOG.warn("orphan workspace {} (parent contextbase missing); dropping", qn);
                    orphanWorkspaces++;
                    continue;
                }
                if (collectionQn == null || !collections.containsKey(collectionQn)) {
                    LOG.warn("orphan workspace {} (parent collection {} missing); dropping",
                            qn, collectionQn);
                    orphanWorkspaces++;
                    continue;
                }
                workspaces.put(qn, m);
            }
            if (orphanWorkspaces > 0) {
                LOG.warn("dropped {} orphan workspaces during image load", orphanWorkspaces);
            }
            int rpCount = reader.readInt();
            for (int i = 0; i < rpCount; i++) {
                RetrievalProfileMeta m = reader.readJson(RetrievalProfileMeta.class);
                retrievalProfiles.put(m.getName(), m);
            }
            LOG.info("loaded context mgr: {} contextbases, {} collections, {} workspaces, {} profiles",
                    cbCount, colCount, wsCount, rpCount);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private String findContextBaseName(long id) {
        ContextBaseMeta m = contextBasesById.get(id);
        return m == null ? null : m.getName();
    }
}
