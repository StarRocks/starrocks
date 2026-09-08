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

package com.starrocks.catalog;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Writable;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** An immutable, credential-free revision of a cluster-wide AI model. */
public final class AIModel implements Writable {
    private static final Set<String> PROPERTY_NAMES =
            Set.of("capability", "provider", "endpoint", "model", "credential_ref");

    public enum Capability {
        CHAT, TEXT_EMBEDDING
    }

    public enum Provider {
        @SerializedName("openai_compatible")
        OPENAI_COMPATIBLE;

        public String getSqlName() {
            return "openai_compatible";
        }
    }

    @SerializedName("id")
    private final long id;
    @SerializedName("name")
    private final String name;
    @SerializedName("revision")
    private final long revision;
    @SerializedName("capability")
    private final Capability capability;
    @SerializedName("provider")
    private final Provider provider;
    @SerializedName("endpoint")
    private final String endpoint;
    @SerializedName("remoteModel")
    private final String remoteModel;
    @SerializedName("credentialRef")
    private final String credentialRef;
    @SerializedName("comment")
    private final String comment;

    private AIModel(long id, String name, long revision, Capability capability, Provider provider,
                    String endpoint, String remoteModel, String credentialRef, String comment) {
        this.id = id;
        this.name = name;
        this.revision = revision;
        this.capability = capability;
        this.provider = provider;
        this.endpoint = endpoint;
        this.remoteModel = remoteModel;
        this.credentialRef = credentialRef;
        this.comment = comment;
    }

    public static AIModel create(long id, String name, Map<String, String> properties, String comment) throws DdlException {
        if (id <= 0) {
            throw new DdlException("AI model id must be positive");
        }
        requireValue("name", name);
        validateCreateProperties(properties);
        return new AIModel(id, name, 1, Capability.valueOf(properties.get("capability")), Provider.OPENAI_COMPATIBLE,
                properties.get("endpoint"), properties.get("model"), properties.get("credential_ref"),
                comment == null ? "" : comment);
    }

    public static void validateCreateProperties(Map<String, String> properties) throws DdlException {
        validateAlterProperties(properties);
        for (String key : PROPERTY_NAMES) {
            requireValue(key, properties.get(key));
        }
    }

    public static void validateAlterProperties(Map<String, String> properties) throws DdlException {
        if (properties == null) {
            throw new DdlException("AI model properties must be set");
        }
        for (Map.Entry<String, String> property : properties.entrySet()) {
            String key = property.getKey();
            if ("api_key".equalsIgnoreCase(key)) {
                throw new DdlException("AI models do not accept api_key; use credential_ref");
            }
            if (key == null || !PROPERTY_NAMES.contains(key)) {
                throw new DdlException("Unsupported AI model property");
            }
            requireValue(key, property.getValue());
        }
        if (properties.containsKey("capability")
                && !Set.of("CHAT", "TEXT_EMBEDDING").contains(properties.get("capability"))) {
            throw new DdlException("AI model capability must be CHAT or TEXT_EMBEDDING");
        }
        if (properties.containsKey("provider")
                && !Provider.OPENAI_COMPATIBLE.getSqlName().equals(properties.get("provider"))) {
            throw new DdlException("AI model provider must be openai_compatible");
        }
        if (properties.containsKey("credential_ref") && !properties.get("credential_ref").matches("[A-Z0-9_]{1,64}")) {
            throw new DdlException("AI model credential_ref must match [A-Z0-9_]{1,64}");
        }
        if (properties.containsKey("endpoint")) {
            validateEndpoint(properties.get("endpoint"));
        }
    }

    public AIModel withAlteredProperties(Map<String, String> updates, String newComment) throws DdlException {
        validateAlterProperties(updates);
        if (updates.containsKey("capability") && !capability.name().equals(updates.get("capability"))) {
            throw new DdlException("Cannot modify AI model capability; create a new model instead");
        }
        if (updates.containsKey("credential_ref") && !credentialRef.equals(updates.get("credential_ref"))) {
            throw new DdlException("Cannot modify AI model credential_ref; create a new model instead");
        }
        String nextEndpoint = updates.getOrDefault("endpoint", endpoint);
        String nextModel = updates.getOrDefault("model", remoteModel);
        String nextComment = newComment == null ? comment : newComment;
        if (endpoint.equals(nextEndpoint) && remoteModel.equals(nextModel) && comment.equals(nextComment)) {
            return this;
        }
        if (revision == Long.MAX_VALUE) {
            throw new DdlException("AI model revision limit reached");
        }
        return new AIModel(id, name, revision + 1, capability, provider, nextEndpoint, nextModel, credentialRef, nextComment);
    }

    public void validatePersistedState() throws IOException {
        try {
            if (id <= 0 || revision <= 0 || capability == null || provider == null || comment == null) {
                throw new DdlException("Invalid AI model identity or typed metadata");
            }
            requireValue("name", name);
            Map<String, String> properties = new HashMap<>();
            properties.put("capability", capability.name());
            properties.put("provider", provider.getSqlName());
            properties.put("endpoint", endpoint);
            properties.put("model", remoteModel);
            properties.put("credential_ref", credentialRef);
            validateCreateProperties(properties);
        } catch (DdlException e) {
            throw new IOException("Invalid persisted AI model metadata", e);
        }
    }

    private static void requireValue(String key, String value) throws DdlException {
        if (value == null || value.isBlank() || value.chars().anyMatch(ch -> ch <= 0x1f || ch == 0x7f)) {
            throw new DdlException("AI model property '" + key + "' must be nonblank without control characters");
        }
    }

    private static void validateEndpoint(String value) throws DdlException {
        try {
            URI uri = new URI(value);
            String authority = uri.getRawAuthority();
            if (!"https".equalsIgnoreCase(uri.getScheme()) || uri.getHost() == null
                    || (uri.getPort() != -1 && (uri.getPort() < 1 || uri.getPort() > 65535))
                    || (authority != null && authority.endsWith(":")) || uri.getRawUserInfo() != null
                    || uri.getRawQuery() != null || uri.getRawFragment() != null) {
                throw new URISyntaxException("", "");
            }
        } catch (URISyntaxException e) {
            throw new DdlException("AI model endpoint must be a complete HTTPS URL without userinfo, query or fragment");
        }
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public long getRevision() {
        return revision;
    }

    public Capability getCapability() {
        return capability;
    }

    public Provider getProvider() {
        return provider;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getRemoteModel() {
        return remoteModel;
    }

    public String getCredentialRef() {
        return credentialRef;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof AIModel model)) {
            return false;
        }
        return id == model.id && revision == model.revision && name.equals(model.name)
                && capability == model.capability && provider == model.provider && endpoint.equals(model.endpoint)
                && remoteModel.equals(model.remoteModel) && credentialRef.equals(model.credentialRef)
                && comment.equals(model.comment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, revision, capability, provider, endpoint, remoteModel, credentialRef, comment);
    }
}
