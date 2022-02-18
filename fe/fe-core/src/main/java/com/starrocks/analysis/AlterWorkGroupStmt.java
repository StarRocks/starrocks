// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.google.common.base.Preconditions;
import com.starrocks.sql.ast.Relation;

import java.util.List;
import java.util.Map;

// Alter WorkGroup specified by name
// 1. Add a new classifier to the WorkGroup
//  ALTER RESOURCE_GROUP <name> ADD (user='<user>', role='<role>', query_type in (...), source_ip='<cidr>')
//
// 2. Drop present classifiers by their ids
//  ALTER RESOURCE_GROUP <name> DROP (<id_1>, <id_2>, ...)
//  ALTER RESOURCE_GROUP <name> DROP ALL
//
// 3. Modify properties
//  ALTER RESOURCE_GROUP <name> WITH ('cpu_core_limit'='n', 'mem_limit'='m%', 'concurrency_limit'='k')
public class AlterWorkGroupStmt extends DdlStmt {
    private String name;
    private SubCommand cmd;

    public AlterWorkGroupStmt(String name, SubCommand cmd) {
        this.name = name;
        this.cmd = cmd;
    }

    public SubCommand getCmd() {
        return this.cmd;
    }

    public String getName() {
        return this.name;
    }

    public Relation analyze() {
        return null;
    }

    public List<Long> getClassifiersToDrop() {
        Preconditions.checkArgument(cmd instanceof DropClassifiers);
        DropClassifiers dropClassifiers = (DropClassifiers) cmd;
        return dropClassifiers.classifierIds;
    }

    public static class SubCommand {

    }

    public static class AddClassifiers extends SubCommand {
        private List<List<Predicate>> classifiers;

        public AddClassifiers(List<List<Predicate>> classifiers) {
            this.classifiers = classifiers;
        }
    }

    public static class DropClassifiers extends SubCommand {
        private List<Long> classifierIds;

        public DropClassifiers(List<Long> classifierIds) {
            this.classifierIds = classifierIds;
        }
    }

    public static class DropAllClassifiers extends SubCommand {
        public DropAllClassifiers() {
        }
    }

    public static class AlterProperties extends SubCommand {
        private Map<String, String> properties;

        public AlterProperties(Map<String, String> properties) {
            this.properties = properties;
        }
    }
}
