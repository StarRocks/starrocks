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
import com.google.gson.annotations.SerializedName;
import com.staros.util.LockCloseable;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.DdlException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.io.Writable;
import com.starrocks.context.ai.AIProvider;
import com.starrocks.context.ai.AIProviderType;
import com.starrocks.epack.persist.EditLogEPack;
import com.starrocks.persist.DropAIProviderLog;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.SetDefaultAIProviderLog;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * In-memory and persisted registry of {@link AIProvider} objects of every type (embedding / rerank /
 * future text). One registry holds them all and keeps one default <b>per type</b>, so the three (and
 * future) provider kinds share the same DDL, persistence and credential handling instead of each
 * duplicating the machinery.
 *
 * <p>Persisted as image block {@link SRMetaBlockID#AI_PROVIDER_MGR} and replayed via the
 * {@code OP_*_AI_PROVIDER} edit-log ops. A provider record with no {@code type} tag and the
 * single {@code defaultProviderId} field are read as an EMBEDDING provider / EMBEDDING default —
 * {@link #gsonPostProcess()} folds them into {@link #defaultByType}, and the EMBEDDING default is
 * mirrored back to {@code defaultProviderId} so the embedding code path can resolve it directly.
 */
public class AIProviderMgr implements Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(AIProviderMgr.class);

    // Mirror of the EMBEDDING default id, kept so the embedding code path can resolve the default
    // without consulting defaultByType. Authoritative per-type defaults live in defaultByType.
    @SerializedName("defaultProviderId")
    protected String defaultProviderId = "";

    // One default provider id per type. Authoritative going forward.
    @SerializedName("defaultByType")
    protected Map<AIProviderType, String> defaultByType = new EnumMap<>(AIProviderType.class);

    @SerializedName("idToProvider")
    protected Map<String, AIProvider> idToProvider = new HashMap<>();

    protected final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public String createProvider(String name, AIProviderType type, Map<String, String> params, String comment)
            throws AlreadyExistsException, DdlException {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            if (existsNoLock(name)) {
                throw new AlreadyExistsException(String.format("AI provider '%s' already exists", name));
            }
            String id = UUID.randomUUID().toString();
            AIProvider provider = new AIProvider(id, name, type, params, comment);
            ((EditLogEPack) GlobalStateMgr.getCurrentState().getEditLog())
                    .logCreateAIProvider(provider, wal -> idToProvider.put(id, provider));
            return id;
        }
    }

    public void alterProvider(String name, Map<String, String> params, boolean ifExists)
            throws DdlException, MetaNotFoundException {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            AIProvider existing = getProviderByNameNoLock(name);
            if (existing == null) {
                if (ifExists) {
                    return;
                }
                throw new MetaNotFoundException(String.format("AI provider '%s' does not exist", name));
            }
            AIProvider updated = new AIProvider(existing);
            updated.mergeParams(params);
            ((EditLogEPack) GlobalStateMgr.getCurrentState().getEditLog())
                    .logAlterAIProvider(updated, wal -> idToProvider.put(updated.getId(), updated));
        }
    }

    public void dropProvider(String name, boolean ifExists)
            throws DdlException, MetaNotFoundException {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            AIProvider existing = getProviderByNameNoLock(name);
            if (existing == null) {
                if (ifExists) {
                    return;
                }
                throw new MetaNotFoundException(String.format("AI provider '%s' does not exist", name));
            }
            Preconditions.checkState(!existing.getId().equals(defaultByType.get(existing.getType())),
                    "Default %s provider cannot be dropped; SET another provider as DEFAULT first",
                    existing.getType().lower());
            DropAIProviderLog log = new DropAIProviderLog(existing.getId());
            ((EditLogEPack) GlobalStateMgr.getCurrentState().getEditLog())
                    .logDropAIProvider(log, wal -> idToProvider.remove(existing.getId()));
        }
    }

    public void setDefaultProvider(String name) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            AIProvider provider = getProviderByNameNoLock(name);
            Preconditions.checkState(provider != null, "AI provider '%s' does not exist", name);
            SetDefaultAIProviderLog log = new SetDefaultAIProviderLog(provider.getId());
            ((EditLogEPack) GlobalStateMgr.getCurrentState().getEditLog())
                    .logSetDefaultAIProvider(log, wal -> applyDefaultNoLock(provider.getId()));
        }
    }

    /** Default embedding provider (convenience no-arg form used by the embedding code path). */
    public AIProvider getDefaultProvider() {
        return getDefaultProvider(AIProviderType.EMBEDDING);
    }

    public AIProvider getProvider(String name) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return getProviderByNameNoLock(name);
        }
    }

    /** Default provider of the given type, or null. */
    public AIProvider getDefaultProvider(AIProviderType type) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            String id = defaultByType.get(type);
            return Strings.isNullOrEmpty(id) ? null : idToProvider.get(id);
        }
    }

    /** Default provider id of the given type ("" if none). */
    public String getDefaultProviderId(AIProviderType type) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            String id = defaultByType.get(type);
            return id == null ? "" : id;
        }
    }

    public List<AIProvider> listProviders() {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return new ArrayList<>(idToProvider.values());
        }
    }

    public List<AIProvider> listProviders(AIProviderType type) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            List<AIProvider> out = new ArrayList<>();
            for (AIProvider p : idToProvider.values()) {
                if (p.getType() == type) {
                    out.add(p);
                }
            }
            return out;
        }
    }

    public boolean exists(String name) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return existsNoLock(name);
        }
    }

    private AIProvider getProviderByNameNoLock(String name) {
        for (AIProvider provider : idToProvider.values()) {
            if (provider.getName().equals(name)) {
                return provider;
            }
        }
        return null;
    }

    private boolean existsNoLock(String name) {
        return getProviderByNameNoLock(name) != null;
    }

    // Set the default for the type of the provider identified by id (type inferred from the stored
    // provider, so the set-default log only needs to carry the id — works for legacy embedding logs too).
    private void applyDefaultNoLock(String id) {
        AIProvider provider = idToProvider.get(id);
        AIProviderType type = provider != null ? provider.getType() : AIProviderType.EMBEDDING;
        defaultByType.put(type, id);
        if (type == AIProviderType.EMBEDDING) {
            defaultProviderId = id;
        }
    }

    public void replayCreateProvider(AIProvider provider) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            normalizeType(provider);
            idToProvider.put(provider.getId(), provider);
        }
    }

    public void replayAlterProvider(AIProvider provider) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            normalizeType(provider);
            idToProvider.put(provider.getId(), provider);
        }
    }

    public void replayDropProvider(DropAIProviderLog log) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            idToProvider.remove(log.getId());
        }
    }

    public void replaySetDefaultProvider(SetDefaultAIProviderLog log) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            applyDefaultNoLock(log.getId());
        }
    }

    private static void normalizeType(AIProvider provider) {
        if (provider != null) {
            // getType() already maps null -> EMBEDDING; persist that decision on the object.
            provider.setType(provider.getType());
        }
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.AI_PROVIDER_MGR, 1);
        writer.writeJson(this);
        writer.close();
    }

    public void load(SRMetaBlockReader reader)
            throws SRMetaBlockEOFException, IOException, SRMetaBlockException {
        AIProviderMgr data = reader.readJson(AIProviderMgr.class);
        this.defaultProviderId = data.defaultProviderId;
        this.defaultByType = data.defaultByType;
        this.idToProvider = data.idToProvider;
        gsonPostProcess();
    }

    @Override
    public void gsonPostProcess() throws IOException {
        if (idToProvider == null) {
            idToProvider = new HashMap<>();
        }
        if (defaultByType == null) {
            defaultByType = new EnumMap<>(AIProviderType.class);
        }
        if (defaultProviderId == null) {
            defaultProviderId = "";
        }
        // Tag legacy (pre-unification) providers, which were all embedding providers, as EMBEDDING.
        for (AIProvider p : idToProvider.values()) {
            normalizeType(p);
        }
        // Migrate the old single embedding default into the per-type map.
        if (!Strings.isNullOrEmpty(defaultProviderId)
                && !defaultByType.containsKey(AIProviderType.EMBEDDING)) {
            defaultByType.put(AIProviderType.EMBEDDING, defaultProviderId);
        }
    }
}
