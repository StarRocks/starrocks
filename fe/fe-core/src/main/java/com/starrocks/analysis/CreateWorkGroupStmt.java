package com.starrocks.analysis;

import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.Relation;

import java.util.List;
import java.util.Map;

// WorkGroup create statement format
// create resource_group [if not exists] [or replace] <name>
// to
//  (user='foobar1', role='foo1', query_type in ('select'), source_ip='192.168.1.1/24'),
//  (user='foobar2', role='foo2', query_type in ('insert'), source_ip='192.168.2.1/24')
// with ('cpu_core_limit'='n', 'mem_limit'='m%', 'concurrency_limit'='n', 'type' = 'normal');
//
public class CreateWorkGroupStmt extends DdlStmt {
    private String name;
    private boolean ifNotExists;
    private boolean replaceIfExists;
    private List<List<Predicate>> classifiers;
    private Map<String, String> properties;

    public CreateWorkGroupStmt(String name, boolean ifNotExists, boolean replaceIfExists, List<List<Predicate>> classifiers, Map<String, String> proeprties) {
        this.name = name;
        this.ifNotExists = ifNotExists;
        this.replaceIfExists = replaceIfExists;
        this.classifiers = classifiers;
        this.properties = proeprties;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public boolean isReplaceIfExists() {
        return replaceIfExists;
    }

    public void setReplaceIfExists(boolean replaceIfExists) {
        this.replaceIfExists = replaceIfExists;
    }

    public Relation analyze() throws SemanticException {
        return null;
    }
}
