// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.starrocks.thrift.TWorkGroupClassifier;
import org.apache.commons.net.util.SubnetUtils;

import java.util.Arrays;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class WorkGroupClassifier {
    public static final Pattern UseRolePattern = Pattern.compile("^\\w+$");
    public static final Set<String> QUERY_TYPES = Arrays.stream(QueryType.values()).map(Enum::name).collect(Collectors.toSet());
    private long id;
    private String user;
    private String role;
    private Set<QueryType> queryTypes;
    private SubnetUtils.SubnetInfo sourceIp;
    private long workgroupId;

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

    public SubnetUtils.SubnetInfo getSourceIp() {
        return sourceIp;
    }

    public void setSourceIp(SubnetUtils.SubnetInfo sourceIp) {
        this.sourceIp = sourceIp;
    }

    public TWorkGroupClassifier toThrift() {
        TWorkGroupClassifier classifier = new TWorkGroupClassifier();
        classifier.setId(id);
        classifier.setWorkgroup_id(workgroupId);
        if (user != null) {
            classifier.setUser(user);
        }
        if (role != null) {
            classifier.setRole(role);
        }
        if (queryTypes != null) {
            classifier.setQuery_types(queryTypes.stream().map(QueryType::name).collect(Collectors.toSet()));
        }
        if (sourceIp != null) {
            classifier.setSource_ip(sourceIp.getCidrSignature());
        }
        return classifier;
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
