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

package com.starrocks.authentication;

import com.google.common.collect.ImmutableSet;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.DdlException;
import com.starrocks.sql.analyzer.SemanticException;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public abstract class GroupProvider {
    public static final String GROUP_PROVIDER_PROPERTY_TYPE_KEY = "type";

    @SerializedName(value = "n")
    protected String name;
    @SerializedName(value = "m")
    protected Map<String, String> properties;

    public GroupProvider(String name, Map<String, String> properties) {
        this.name = name;
        this.properties = properties;
    }

    public void init() throws DdlException {

    }

    public void destroy() {

    }

    /**
     * Synchronously verify that this provider can serve requests under its current configuration,
     * and warm up any in-memory cache, so that the moment it is swapped into service
     * (e.g. by ALTER GROUP PROVIDER) it returns correct data instead of an empty result.
     *
     * <p>Implementations that talk to a remote directory (LDAP) override this to perform a
     * one-shot, blocking lookup that throws {@link DdlException} when the configuration is not
     * usable. It must NOT start any recurring background task; starting the periodic schedule is
     * {@link #init()}'s job. This is only invoked on the leader's DDL path, never during journal
     * replay, so it is safe for it to block on network I/O.
     */
    public void prepareForActivation() throws DdlException {

    }

    /**
     * Called on the replay path right before this instance replaces {@code previous} in the manager's
     * map. Implementations with a cache that takes a directory round trip to fill may answer lookups
     * through {@code previous} until their own first refresh has completed, so a follower applying an
     * ALTER never resolves an empty group set in between. The leader does not need this: its ALTER warms
     * the replacement synchronously in {@link #prepareForActivation()} before publishing it.
     *
     * <p>Queries are forwarded, not data: the outgoing instance computes lookup keys with its own
     * configuration against the cache it built itself, so a change to the key encoding
     * ({@code ldap_user_search_attr}) between the two configurations cannot mismatch.
     *
     * <p>Default is a no-op, which is right for providers that are already warm when {@link #init()}
     * returns (file) or hold no cache at all (unix).
     */
    public void serveFromUntilWarm(GroupProvider previous) {

    }

    public String getName() {
        return name;
    }

    public String getType() {
        return properties.get("type");
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getComment() {
        return "";
    }

    /**
     * Every property key this implementation reads, in its canonical (lower case) spelling. Used by
     * ALTER to reject a property the provider would never look at: {@code properties} is a plain map and
     * every getter reads it with an exact {@code get()}, so a typo'd or differently spelled key would be
     * stored next to the real one and the statement would report success while changing nothing - the
     * failure mode that matters most for a password rotation.
     *
     * <p>Only ALTER checks this. CREATE still accepts any key, so a provider may legitimately carry
     * properties that are not listed here; the check is on what a single ALTER adds, not on what the
     * provider already holds.
     */
    public Set<String> getKnownPropertyKeys() {
        return ImmutableSet.of(GROUP_PROVIDER_PROPERTY_TYPE_KEY);
    }

    /**
     * Validates the keys of one ALTER delta and returns the delta with every key in its canonical spelling.
     *
     * <p>A key is accepted if the type defines it ({@link #getKnownPropertyKeys()}) or this provider already
     * has it - CREATE stores whatever it is given, so a provider may carry properties no implementation
     * reads, and ALTER must still be able to update them. Anything else is refused: it would be stored next
     * to the property the user meant to change, and the statement would report success having changed
     * nothing. That is the whole failure mode of a password rotation typed as
     * {@code ldap_bind_root_pw}.
     *
     * <p>Keys are matched ignoring case and normalized, because case is the one spelling difference a user
     * can plausibly get wrong without noticing - {@code SET ("LDAP_BIND_ROOT_PWD" = ...)} reads as correct -
     * while every getter looks the property up with an exact {@code get()}.
     */
    public Map<String, String> canonicalizeAlterProperties(Map<String, String> alterProps) throws SemanticException {
        Map<String, String> canonicalKeys = new HashMap<>();
        for (String known : getKnownPropertyKeys()) {
            canonicalKeys.put(known.toLowerCase(Locale.ROOT), known);
        }
        for (String present : properties.keySet()) {
            canonicalKeys.putIfAbsent(present.toLowerCase(Locale.ROOT), present);
        }

        Map<String, String> canonical = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : alterProps.entrySet()) {
            String canonicalKey = canonicalKeys.get(entry.getKey().toLowerCase(Locale.ROOT));
            if (canonicalKey == null) {
                throw new SemanticException("unknown property '" + entry.getKey() + "' for group provider '" + name
                        + "': a provider of type '" + getType() + "' does not define it and this provider does not"
                        + " have it. The properties of this type are: " + new TreeSet<>(getKnownPropertyKeys()));
            }
            canonical.put(canonicalKey, entry.getValue());
        }
        return canonical;
    }

    public abstract Set<String> getGroup(UserIdentity userIdentity, String distinguishedName);

    public abstract void checkProperty() throws SemanticException;
}
