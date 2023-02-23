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

import com.google.common.collect.ImmutableSet;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.net.util.SubnetUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ResourceGroupClassifier implements Writable {
    public static final Pattern USE_ROLE_PATTERN = Pattern.compile("^\\w+$");
    public static final ImmutableSet<String> SUPPORTED_QUERY_TYPES =
            ImmutableSet.of(QueryType.SELECT.name(), QueryType.INSERT.name());

    @SerializedName(value = "id")
    private long id;
    @SerializedName(value = "user")
    private String user;
    @SerializedName(value = "role")
    private String role;
    @SerializedName(value = "queryTypes")
    private Set<QueryType> queryTypes;
    @SerializedName(value = "sourceIp")
    private String sourceIp;
    @SerializedName(value = "workgroupId")
    private long resourceGroupId;
    @SerializedName(value = "databaseIds")
    private Set<Long> databaseIds;

    public static ResourceGroupClassifier read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ResourceGroupClassifier.class);
    }

    public long getResourceGroupId() {
        return resourceGroupId;
    }

    public void setResourceGroupId(long resourceGroupId) {
        this.resourceGroupId = resourceGroupId;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public Set<QueryType> getQueryTypes() {
        return queryTypes;
    }

    public void setQueryTypes(Set<QueryType> queryTypes) {
        this.queryTypes = queryTypes;
    }

    public String getSourceIp() {
        return sourceIp;
    }

    public void setSourceIp(String sourceIp) {
        this.sourceIp = sourceIp;
    }

    public void setDatabases(List<Long> databases) {
        this.databaseIds = new HashSet<>(databases);
    }

    public Set<Long> getDatabases() {
        return this.databaseIds;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public boolean isSatisfied(String user, List<String> activeRoles, QueryType queryType, String sourceIp,
                               Set<Long> dbIds) {
        if (!isVisible(user, activeRoles, sourceIp)) {
            return false;
        }
        if (CollectionUtils.isNotEmpty(queryTypes) && !this.queryTypes.contains(queryType)) {
            return false;
        }
        if (CollectionUtils.isNotEmpty(databaseIds)) {
            return CollectionUtils.isNotEmpty(dbIds) && databaseIds.containsAll(dbIds);
        }

        return true;
    }

    public boolean isVisible(String user, List<String> activeRoles, String sourceIp) {
        if (this.user != null && !this.user.equals(user)) {
            return false;
        }
        if (this.role != null && !activeRoles.contains(role)) {
            return false;
        }
        if (this.sourceIp != null && sourceIp != null) {
            return new SubnetUtils(this.sourceIp).getInfo().isInRange(sourceIp);
        }
        return true;
    }

    public double weight() {
        double w = 0;
        if (user != null) {
            w += 1;
        }
        if (role != null) {
            w += 1;
        }
        if (queryTypes != null && !queryTypes.isEmpty()) {
            w += 1 + 0.1 / queryTypes.size();
        }
        if (sourceIp != null) {
            SubnetUtils.SubnetInfo subnetInfo = new SubnetUtils(sourceIp).getInfo();
            w += 1 + (Long.numberOfLeadingZeros(subnetInfo.getAddressCountLong() + 2) - 32) / 64.0;
        }
        if (CollectionUtils.isNotEmpty(databaseIds)) {
            w += 10.0 * databaseIds.size();
        }
        return w;
    }

    @Override
    public String toString() {
        StringBuilder classifiersStr = new StringBuilder();
        classifiersStr.append("(");
        classifiersStr.append("id=" + id);
        classifiersStr.append(", weight=" + weight());
        if (user != null) {
            classifiersStr.append(", user=" + user);
        }
        if (role != null) {
            classifiersStr.append(", role=" + role);
        }
        if (CollectionUtils.isNotEmpty(queryTypes)) {
            List<String> queryTypeList = queryTypes.stream().
                    map(QueryType::name).sorted(String::compareTo).collect(Collectors.toList());
            String queryTypesStr = String.join(", ", queryTypeList);
            classifiersStr.append(", query_type in (" + queryTypesStr + ")");
        }
        if (CollectionUtils.isNotEmpty(databaseIds)) {
            String str = databaseIds.stream()
                    .map(id ->
                            Optional.ofNullable(GlobalStateMgr.getCurrentState().getDb(id))
                                    .map(Database::getFullName)
                                    .orElse("unknown"))
                    .collect(Collectors.joining(","));
            classifiersStr.append(", db='" + str + "'");
        }
        if (sourceIp != null) {
            classifiersStr.append(", source_ip=" + sourceIp);
        }
        classifiersStr.append(")");
        return classifiersStr.toString();
    }

    public enum QueryType {
        SELECT,
        CTAS,
        INSERT,
        COPY,
        EXPORT,
        UPDATE,
        DELETE,
        COMPACTION,
        SCHEMA_CHANGE,
        CLONE,
        MV,
        SYSTEM_OTHER
    }
}
