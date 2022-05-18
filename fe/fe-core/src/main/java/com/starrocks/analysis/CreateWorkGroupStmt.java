package com.starrocks.analysis;

import com.starrocks.catalog.WorkGroup;
import com.starrocks.catalog.WorkGroupClassifier;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.analyzer.WorkGroupAnalyzer;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.thrift.TWorkGroupType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// WorkGroup create statement format
// create resource group [if not exists] [or replace] <name>
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
    private WorkGroup workgroup;

    public CreateWorkGroupStmt(String name, boolean ifNotExists, boolean replaceIfExists,
                               List<List<Predicate>> classifiers, Map<String, String> proeprties) {
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

    public void analyze() throws SemanticException {
        workgroup = new WorkGroup();
        workgroup.setName(name);
        List<WorkGroupClassifier> classifierList = new ArrayList<>();
        for (List<Predicate> predicates : classifiers) {
            WorkGroupClassifier classifier = WorkGroupAnalyzer.convertPredicateToClassifier(predicates);
            classifierList.add(classifier);
        }
        workgroup.setClassifiers(classifierList);
        WorkGroupAnalyzer.analyzeProperties(workgroup, properties);

        if (workgroup.getWorkGroupType() == null) {
            workgroup.setWorkGroupType(TWorkGroupType.WG_NORMAL);
        }
        if (workgroup.getCpuCoreLimit() == null) {
            throw new SemanticException("property 'cpu_core_limit' is absent");
        }
        if (workgroup.getMemLimit() == null) {
            throw new SemanticException("property 'mem_limit' is absent");
        }
    }

    public WorkGroup getWorkgroup() {
        return workgroup;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateWorkGroupStatement(this, context);
    }
}
