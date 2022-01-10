// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.starrocks.thrift.TWorkGroup;
import com.starrocks.thrift.TWorkGroupClassifier;
import com.starrocks.thrift.TWorkGroupType;
import org.apache.commons.net.util.SubnetUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class WorkGroup {
    public static final String USER = "user";
    public static final String ROLE = "role";
    public static final String QUERY_TYPE = "query_type";
    public static final String SOURCE_IP = "source_ip";
    public static final String CPU_CORE_LIMIT = "cpu_core_limit";
    public static final String MEM_LIMIT = "mem_limit";
    public static final String CONCURRENCY_LIMIT = "concurrency_limit";
    public static final String WORKGROUP_TYPE = "type";

    List<WorkGroupClassifier> classifiers;
    private String name;
    private long id;
    private Integer cpuCoreLimit;
    private Double memLimit;
    private Integer concurrencyLimit;
    private TWorkGroupType workGroupType;

    public WorkGroup() {
    }

    public WorkGroup(TWorkGroup twg) {
        this.name = twg.getName();
        this.id = twg.getId();
        this.cpuCoreLimit = twg.getCpu_core_limit();
        this.memLimit = twg.getMem_limit();
        this.concurrencyLimit = twg.getConcurrency_limit();
        this.workGroupType = twg.getWorkgroup_type();
        List<WorkGroupClassifier> classifierList = new ArrayList<>();
        for (TWorkGroupClassifier tclassifier : twg.getClassifiers()) {
            WorkGroupClassifier classifier = new WorkGroupClassifier();
            if (tclassifier.isSetUser()) {
                classifier.setUser(tclassifier.getUser());
            }
            if (tclassifier.isSetRole()) {
                classifier.setRole(tclassifier.getRole());
            }

            if (tclassifier.isSetQuery_types()) {
                Set<WorkGroupClassifier.QueryType> queryTypes =
                        tclassifier.getQuery_types().stream().map(WorkGroupClassifier.QueryType::valueOf)
                                .collect(Collectors.toSet());
                classifier.setQueryTypes(queryTypes);
            }

            if (tclassifier.isSetSource_ip()) {
                SubnetUtils.SubnetInfo sourceIp = new SubnetUtils(tclassifier.getSource_ip()).getInfo();
                classifier.setSourceIp(sourceIp);
            }
            classifierList.add(classifier);
        }
        this.classifiers = classifierList;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    TWorkGroup toThrift() {
        TWorkGroup twg = new TWorkGroup();
        twg.setName(name);
        twg.setId(id);
        if (cpuCoreLimit != null) {
            twg.setCpu_core_limit(cpuCoreLimit);
        }
        if (memLimit != null) {
            twg.setMem_limit(memLimit);
        }
        if (concurrencyLimit != null) {
            twg.setConcurrency_limit(concurrencyLimit);
        }
        if (workGroupType != null) {
            twg.setWorkgroup_type(workGroupType);
        }
        List<TWorkGroupClassifier> classifierList = new ArrayList<>();
        for (WorkGroupClassifier classifier : classifiers) {
            classifierList.add(classifier.toThrift());
        }
        twg.setClassifiers(classifierList);
        return twg;
    }

    public int getCpuCoreLimit() {
        return cpuCoreLimit;
    }

    public void setCpuCoreLimit(int cpuCoreLimit) {
        this.cpuCoreLimit = cpuCoreLimit;
    }

    public double getMemLimit() {
        return memLimit;
    }

    public void setMemLimit(double memLimit) {
        this.memLimit = memLimit;
    }

    public int getConcurrencyLimit() {
        return concurrencyLimit;
    }

    public void setConcurrencyLimit(int concurrencyLimit) {
        this.concurrencyLimit = concurrencyLimit;
    }

    public TWorkGroupType getWorkGroupType() {
        return workGroupType;
    }

    public void setWorkGroupType(TWorkGroupType workGroupType) {
        this.workGroupType = workGroupType;
    }

    public List<WorkGroupClassifier> getClassifiers() {
        return classifiers;
    }

    public void setClassifiers(List<WorkGroupClassifier> classifiers) {
        this.classifiers = classifiers;
    }
}
