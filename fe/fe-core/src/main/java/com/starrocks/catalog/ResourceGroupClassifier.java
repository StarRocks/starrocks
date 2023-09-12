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
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TQueryType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.net.util.SubnetUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ResourceGroupClassifier {
    public static final Pattern USER_PATTERN = Pattern.compile("^[a-zA-Z][a-zA-Z0-9_]{1,63}/?[.a-zA-Z0-9_-]{0,63}$");
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

    @SerializedName(value = "planCpuCostRange")
    private CostRange planCpuCostRange;

    @SerializedName(value = "planMemCostRange")
    private CostRange planMemCostRange;

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

    public void setPlanCpuCostRange(CostRange planCpuCostRange) {
        this.planCpuCostRange = planCpuCostRange;
    }

    public CostRange getPlanCpuCostRange() {
        return planCpuCostRange;
    }

    public void setPlanMemCostRange(CostRange planMemCostRange) {
        this.planMemCostRange = planMemCostRange;
    }

    public CostRange getPlanMemCostRange() {
        return planMemCostRange;
    }

    public boolean isSatisfied(String user, List<String> activeRoles, QueryType queryType, String sourceIp,
                               Set<Long> dbIds, double planCpuCost, double planMemCost) {
        if (!isVisible(user, activeRoles, sourceIp)) {
            return false;
        }
        if (CollectionUtils.isNotEmpty(queryTypes) && !this.queryTypes.contains(queryType)) {
            return false;
        }
        if (CollectionUtils.isNotEmpty(databaseIds) && !(CollectionUtils.isNotEmpty(dbIds) && databaseIds.containsAll(dbIds))) {
            return false;
        }
        if (planCpuCostRange != null && !planCpuCostRange.contains(planCpuCost)) {
            return false;
        }
        if (planMemCostRange != null && !planMemCostRange.contains(planMemCost)) {
            return false;
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
        if (planCpuCostRange != null) {
            w += 1;
        }
        if (planMemCostRange != null) {
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
        if (planCpuCostRange != null) {
            classifiersStr.append(", ").append(ResourceGroup.PLAN_CPU_COST_RANGE).append("=").append(planCpuCostRange);
        }
        if (planMemCostRange != null) {
            classifiersStr.append(", ").append(ResourceGroup.PLAN_MEM_COST_RANGE).append("=").append(planMemCostRange);
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
        SYSTEM_OTHER;

        public static QueryType fromTQueryType(TQueryType type) {
            return type == TQueryType.LOAD ? INSERT : SELECT;
        }
    }

    /**
     * The cost range.
     * <p> Fow now it always includes the left endpoint {@link #min} and excludes the right endpoint {@link #max}.
     * The pattern is {@code [min, max)}, where min and max are double (including infinity and -infinity) and min must be less
     * than max.
     */
    public static class CostRange {
        private static final String STR_RANGE_REGEX = "^\\s*\\[\\s*(.+?)\\s*,\\s*(.+?)\\s*\\)\\s*$";
        private static final Pattern STR_RANGE_PATTERN = Pattern.compile(STR_RANGE_REGEX, Pattern.CASE_INSENSITIVE);

        public static final String FORMAT_STR_RANGE_MESSAGE = "the format must be '[min, max)' " +
                "where min and max are finite double " +
                "and min must be less than max";

        @SerializedName(value = "min")
        private final double min;
        @SerializedName(value = "max")
        private final double max;

        private CostRange(double min, double max) {
            this.min = min;
            this.max = max;
        }

        public static CostRange fromString(String rangeStr) {
            Matcher matcher = STR_RANGE_PATTERN.matcher(rangeStr);
            if (!matcher.find()) {
                return null;
            }

            try {
                double min = Double.parseDouble(matcher.group(1));
                double max = Double.parseDouble(matcher.group(2));

                if (!Double.isFinite(min) || !Double.isFinite(max)) {
                    return null;
                }

                if (min >= max) {
                    return null;
                }

                return new CostRange(min, max);
            } catch (Exception e) {
                return null;
            }
        }

        public boolean contains(double value) {
            return min <= value && value < max;
        }

        @Override
        public String toString() {
            return "[" + min + ", " + max + ")";
        }
    }
}
