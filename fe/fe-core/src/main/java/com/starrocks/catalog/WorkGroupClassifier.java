// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.net.util.SubnetUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class WorkGroupClassifier implements Writable {
    public static final Pattern UseRolePattern = Pattern.compile("^\\w+$");
    public static final Set<String> QUERY_TYPES =
            Arrays.stream(QueryType.values()).map(Enum::name).collect(Collectors.toSet());
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
    private long workgroupId;
    @SerializedName(value = "database")
    private Set<String> databases;

    public static WorkGroupClassifier read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, WorkGroupClassifier.class);
    }

    public long getWorkgroupId() {
        return workgroupId;
    }

    public void setWorkgroupId(long workgroupId) {
        this.workgroupId = workgroupId;
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

    public void setDatabases(List<String> databases) {
        this.databases = new HashSet<>(databases);
    }

    public Set<String> getDatabases() {
        return this.databases;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public boolean isSatisfied(String user, String role, QueryType queryType, String sourceIp,
                               Set<String> dbNames) {
        if (!isVisible(user, role, sourceIp)) {
            return false;
        }
        if (CollectionUtils.isNotEmpty(queryTypes) && !this.queryTypes.contains(queryType)) {
            return false;
        }
        if (CollectionUtils.isNotEmpty(databases)) {
            return CollectionUtils.isNotEmpty(dbNames) && databases.containsAll(dbNames);
        }

        return true;
    }

    public boolean isVisible(String user, String role, String sourceIp) {
        if (this.user != null && !this.user.equals(user)) {
            return false;
        }
        if (this.role != null && !this.role.equals(role)) {
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
        if (CollectionUtils.isNotEmpty(databases)) {
            w += 10.0 * databases.size();
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
        if (CollectionUtils.isNotEmpty(databases)) {
            String str = String.join(",", databases);
            classifiersStr.append(", database='" + str + "'");
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
