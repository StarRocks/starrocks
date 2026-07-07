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

package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Writable;

import java.util.Map;

/**
 * Single edit-log entry for semantic-context control-plane ops. The same payload shape is used for
 * every op kind (create/alter/drop contextbase, collection, workspace, retrieval profile); the
 * dispatch happens via {@link com.starrocks.persist.OperationType} on replay.
 */
public class ContextOpLog implements Writable {

    @SerializedName("id")
    private long id;

    @SerializedName("p")
    private long parentId;

    @SerializedName("n")
    private String name;

    @SerializedName("q")
    private String qualifiedName;

    @SerializedName("t")
    private String typeTag;

    @SerializedName("props")
    private Map<String, String> properties;

    public ContextOpLog() {
    }

    public static ContextOpLog forContextBase(long id, String name, Map<String, String> properties) {
        ContextOpLog log = new ContextOpLog();
        log.id = id;
        log.name = name;
        log.properties = properties;
        return log;
    }

    public static ContextOpLog forCollection(long id, long contextBaseId, String name, String collectionType,
                                             Map<String, String> properties) {
        ContextOpLog log = new ContextOpLog();
        log.id = id;
        log.parentId = contextBaseId;
        log.name = name;
        log.typeTag = collectionType;
        log.properties = properties;
        return log;
    }

    public static ContextOpLog forWorkspace(long id, long collectionId, String qualifiedName,
                                            Map<String, String> properties) {
        ContextOpLog log = new ContextOpLog();
        log.id = id;
        log.parentId = collectionId;
        log.qualifiedName = qualifiedName;
        log.properties = properties;
        return log;
    }

    public static ContextOpLog forRetrievalProfile(long id, String name, Map<String, String> properties) {
        ContextOpLog log = new ContextOpLog();
        log.id = id;
        log.name = name;
        log.properties = properties;
        return log;
    }

    /**
     * Rename payload for a contextbase. Reuses the existing fields to avoid a schema change:
     * {@code name} carries the current (old) name and {@code qualifiedName} carries the new name.
     * The {@code id} is the stable contextbase id (unchanged by the rename).
     */
    public static ContextOpLog forRename(long id, String oldName, String newName) {
        ContextOpLog log = new ContextOpLog();
        log.id = id;
        log.name = oldName;
        log.qualifiedName = newName;
        return log;
    }

    public static ContextOpLog forName(String name) {
        ContextOpLog log = new ContextOpLog();
        log.name = name;
        return log;
    }

    public static ContextOpLog forQualifiedName(String qualifiedName) {
        ContextOpLog log = new ContextOpLog();
        log.qualifiedName = qualifiedName;
        return log;
    }

    public long getId() {
        return id;
    }

    public long getParentId() {
        return parentId;
    }

    public String getName() {
        return name;
    }

    public String getQualifiedName() {
        return qualifiedName;
    }

    public String getTypeTag() {
        return typeTag;
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}
