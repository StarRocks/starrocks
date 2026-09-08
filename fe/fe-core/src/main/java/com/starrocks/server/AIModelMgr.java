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

import com.staros.util.LockCloseable;
import com.starrocks.catalog.AIModel;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.DdlException;
import com.starrocks.persist.DropAIModelLog;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Owns AI model identity and revision publication independently of external resources. */
public final class AIModelMgr {
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private Map<Long, AIModel> idToModel = new HashMap<>();
    private Map<String, Long> nameToId = new HashMap<>();

    public AIModel getByName(String name) {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            return idToModel.get(nameToId.get(name));
        }
    }

    public AIModel getById(long id) {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            return idToModel.get(id);
        }
    }

    public Map<String, AIModel> getModelsByNames(Collection<String> names) {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            Map<String, AIModel> snapshot = new HashMap<>();
            for (String name : names) {
                AIModel model = idToModel.get(nameToId.get(name));
                if (model != null) {
                    snapshot.put(name, model);
                }
            }
            return Map.copyOf(snapshot);
        }
    }

    public List<AIModel> listModels() {
        List<AIModel> snapshot;
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            snapshot = new ArrayList<>(idToModel.values());
        }
        snapshot.sort(Comparator.comparing(AIModel::getName));
        return List.copyOf(snapshot);
    }

    public AIModel createModel(String name, Map<String, String> properties, String comment, boolean ifNotExists)
            throws DdlException, AlreadyExistsException {
        AIModel.validateCreateProperties(properties);
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            AIModel existing = idToModel.get(nameToId.get(name));
            if (existing != null) {
                if (ifNotExists) {
                    return existing;
                }
                throw new AlreadyExistsException("AI model '" + name + "' already exists");
            }
            AIModel model = AIModel.create(GlobalStateMgr.getCurrentState().getNextId(), name, properties, comment);
            GlobalStateMgr.getCurrentState().getEditLog().logCreateAIModel(model, wal -> publishModel(model));
            return model;
        }
    }

    public AIModel alterModel(AIModel expectedTarget, Map<String, String> updates, String newComment) throws DdlException {
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            AIModel current = requireCurrentTarget(expectedTarget);
            AIModel replacement = current.withAlteredProperties(updates, newComment);
            if (replacement == current) {
                return current;
            }
            GlobalStateMgr.getCurrentState().getEditLog().logAlterAIModel(replacement, wal -> publishModel(replacement));
            return replacement;
        }
    }

    public void dropModel(AIModel expectedTarget) throws DdlException {
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            AIModel current = requireCurrentTarget(expectedTarget);
            DropAIModelLog log = new DropAIModelLog(current.getId());
            GlobalStateMgr.getCurrentState().getEditLog().logDropAIModel(log, wal -> removeModel(current.getId()));
        }
    }

    private AIModel requireCurrentTarget(AIModel expectedTarget) throws DdlException {
        AIModel current = idToModel.get(expectedTarget.getId());
        if (current == null || current.getRevision() != expectedTarget.getRevision()
                || !current.getName().equals(expectedTarget.getName())
                || !Long.valueOf(expectedTarget.getId()).equals(nameToId.get(expectedTarget.getName()))) {
            throw new DdlException("AI model changed concurrently; retry the statement");
        }
        return current;
    }

    // Called only under the manager write lock, after durability or while replaying.
    private void publishModel(AIModel model) {
        idToModel.put(model.getId(), model);
        nameToId.put(model.getName(), model.getId());
    }

    private void removeModel(long id) {
        AIModel removed = idToModel.remove(id);
        if (removed != null) {
            nameToId.remove(removed.getName(), id);
        }
    }

    public void replayCreateModel(AIModel model) {
        validateReplayModel(model);
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            AIModel existing = idToModel.get(model.getId());
            if (model.equals(existing)) {
                return;
            }
            if (existing != null || nameToId.containsKey(model.getName())) {
                throw new IllegalStateException("Conflicting AI model identity during replay");
            }
            publishModel(model);
        }
    }

    public void replayAlterModel(AIModel model) {
        validateReplayModel(model);
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            AIModel current = idToModel.get(model.getId());
            if (model.equals(current)) {
                return;
            }
            if (current == null || !current.getName().equals(model.getName())
                    || current.getRevision() == Long.MAX_VALUE || model.getRevision() != current.getRevision() + 1
                    || current.getCapability() != model.getCapability()
                    || !current.getCredentialRef().equals(model.getCredentialRef())) {
                throw new IllegalStateException("Conflicting AI model revision during replay");
            }
            publishModel(model);
        }
    }

    public void replayDropModel(DropAIModelLog log) {
        if (log.getId() <= 0) {
            throw new IllegalStateException("Invalid AI model id during replay");
        }
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            removeModel(log.getId());
        }
    }

    private static void validateReplayModel(AIModel model) {
        try {
            if (model == null) {
                throw new IOException("Missing AI model metadata");
            }
            model.validatePersistedState();
        } catch (IOException e) {
            throw new IllegalStateException("Invalid AI model metadata during replay", e);
        }
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        List<AIModel> models = listModels();
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.AI_MODEL_MGR, models.size() + 1);
        writer.writeInt(models.size());
        for (AIModel model : models) {
            writer.writeJson(model);
        }
        writer.close();
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        Map<Long, AIModel> restoredById = new HashMap<>();
        Map<String, Long> restoredByName = new HashMap<>();
        int count = reader.readInt();
        if (count < 0) {
            throw new IOException("Invalid AI model count in image");
        }
        for (int i = 0; i < count; i++) {
            AIModel model = reader.readJson(AIModel.class);
            if (model == null) {
                throw new IOException("Missing AI model metadata in image");
            }
            model.validatePersistedState();
            if (restoredById.putIfAbsent(model.getId(), model) != null
                    || restoredByName.putIfAbsent(model.getName(), model.getId()) != null) {
                throw new IOException("Duplicate AI model identity in image");
            }
        }
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            idToModel = restoredById;
            nameToId = restoredByName;
        }
    }
}
