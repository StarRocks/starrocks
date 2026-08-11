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

package com.starrocks.sql.analyzer;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.ast.IndexDef;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.IsNullPredicate;
import com.starrocks.sql.ast.expression.MatchExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SearchDslNode;
import com.starrocks.sql.parser.SearchOptions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.analyzer.IndexAnalyzer.INVERTED_INDEX_IMP_LIB_KEY;
import static com.starrocks.sql.analyzer.IndexAnalyzer.INVERTED_INDEX_LOWER_CASE_KEY;
import static com.starrocks.sql.analyzer.IndexAnalyzer.INVERTED_INDEX_PARSER_KEY;
import static com.starrocks.sql.analyzer.IndexAnalyzer.INVERTED_INDEX_PARSER_NONE;

/** Lowers a parsed search DSL to predicates supported by the standard search mode. */
final class StandardSearchPredicateBuilder {
    private static final int MAX_EXPANDED_NODES = 10_000;

    private final SearchFunctionValidator.RelationContext relationContext;
    private final OlapTable table;
    private final SearchOptions options;
    private final NodePosition position;
    private final Map<String, FieldBinding> bindingCache = new HashMap<>();
    private int expandedNodes;

    StandardSearchPredicateBuilder(SearchFunctionValidator.RelationContext relationContext,
                                   SearchOptions options, NodePosition position) {
        this.relationContext = relationContext;
        this.table = relationContext.getTable();
        this.options = options;
        this.position = position;
    }

    Expr build(SearchDslNode root) {
        List<String> configuredFields = options.getEffectiveFields();
        if (configuredFields.isEmpty()) {
            return buildWithFallback(root, null);
        }
        if (!containsUnqualifiedLeaf(root)) {
            return buildWithFallback(root, null);
        }

        List<FieldBinding> bindings = configuredFields.stream().map(this::bind).collect(Collectors.toList());
        Set<ColumnId> distinctColumns = new HashSet<>();
        for (FieldBinding binding : bindings) {
            if (!distinctColumns.add(binding.column.getColumnId())) {
                throw new SemanticException("search() option 'fields' resolves the same column more than once",
                        position);
            }
        }
        if (bindings.size() == 1) {
            return buildWithFallback(root, bindings.get(0));
        }
        if (options.getMultiFieldType() == SearchOptions.MultiFieldType.CROSS_FIELDS) {
            validateCrossFieldAnalyzerSettings(bindings);
        }
        if (options.getMultiFieldType() == SearchOptions.MultiFieldType.CROSS_FIELDS) {
            return buildCrossFields(root, bindings);
        }
        return buildBestFields(root, bindings);
    }

    /**
     * Builds the complete subtree independently for every candidate field. A NOT subtree
     * exits the current field context and is lowered through this method again, making the
     * negation a field-independent atom without breaking the surrounding same-field skeleton.
     */
    private Expr buildBestFields(SearchDslNode node, List<FieldBinding> fields) {
        if (!containsUnqualifiedLeaf(node)) {
            return buildWithFallback(node, null);
        }
        if (node instanceof SearchDslNode.Not) {
            Expr child = buildBestFields(((SearchDslNode.Not) node).getChild(), fields);
            return account(new CompoundPredicate(CompoundPredicate.Operator.NOT, child, null, position));
        }
        return combine(CompoundPredicate.Operator.OR,
                fields.stream().map(field -> buildBestFieldsForField(node, field, fields))
                        .collect(Collectors.toList()));
    }

    private Expr buildBestFieldsForField(SearchDslNode node, FieldBinding field, List<FieldBinding> fields) {
        if (node instanceof SearchDslNode.Term) {
            SearchDslNode.Term term = (SearchDslNode.Term) node;
            return buildTerm(term, bindingFor(term.getField(), field));
        }
        if (node instanceof SearchDslNode.Function) {
            SearchDslNode.Function function = (SearchDslNode.Function) node;
            return buildFunction(function, bindingFor(function.getField(), field));
        }
        if (node instanceof SearchDslNode.Implicit) {
            SearchDslNode.Implicit implicit = (SearchDslNode.Implicit) node;
            List<Expr> children = implicit.getChildren().stream()
                    .map(child -> buildBestFieldsForField(child, field, fields)).collect(Collectors.toList());
            return combine(defaultCompoundOperator(), children);
        }
        if (node instanceof SearchDslNode.And) {
            List<Expr> children = ((SearchDslNode.And) node).getChildren().stream()
                    .map(child -> buildBestFieldsForField(child, field, fields)).collect(Collectors.toList());
            return combine(CompoundPredicate.Operator.AND, children);
        }
        if (node instanceof SearchDslNode.Or) {
            List<Expr> children = ((SearchDslNode.Or) node).getChildren().stream()
                    .map(child -> buildBestFieldsForField(child, field, fields)).collect(Collectors.toList());
            return combine(CompoundPredicate.Operator.OR, children);
        }
        if (node instanceof SearchDslNode.Not) {
            Expr child = buildBestFields(((SearchDslNode.Not) node).getChild(), fields);
            return account(new CompoundPredicate(CompoundPredicate.Operator.NOT, child, null, position));
        }
        throw new IllegalStateException("unknown search DSL node: " + node.getClass().getSimpleName());
    }

    private Expr buildWithFallback(SearchDslNode node, FieldBinding fallback) {
        if (node instanceof SearchDslNode.Term) {
            SearchDslNode.Term term = (SearchDslNode.Term) node;
            return buildTerm(term, bindingFor(term.getField(), fallback));
        }
        if (node instanceof SearchDslNode.Function) {
            SearchDslNode.Function function = (SearchDslNode.Function) node;
            return buildFunction(function, bindingFor(function.getField(), fallback));
        }
        if (node instanceof SearchDslNode.Implicit) {
            SearchDslNode.Implicit implicit = (SearchDslNode.Implicit) node;
            List<Expr> children = implicit.getChildren().stream()
                    .map(child -> buildWithFallback(child, fallback)).collect(Collectors.toList());
            return combine(defaultCompoundOperator(), children);
        }
        if (node instanceof SearchDslNode.And) {
            List<Expr> children = ((SearchDslNode.And) node).getChildren().stream()
                    .map(child -> buildWithFallback(child, fallback)).collect(Collectors.toList());
            return combine(CompoundPredicate.Operator.AND, children);
        }
        if (node instanceof SearchDslNode.Or) {
            List<Expr> children = ((SearchDslNode.Or) node).getChildren().stream()
                    .map(child -> buildWithFallback(child, fallback)).collect(Collectors.toList());
            return combine(CompoundPredicate.Operator.OR, children);
        }
        if (node instanceof SearchDslNode.Not) {
            Expr child = buildWithFallback(((SearchDslNode.Not) node).getChild(), fallback);
            return account(new CompoundPredicate(CompoundPredicate.Operator.NOT, child, null, position));
        }
        throw new IllegalStateException("unknown search DSL node: " + node.getClass().getSimpleName());
    }

    private Expr buildCrossFields(SearchDslNode node, List<FieldBinding> fields) {
        if (node instanceof SearchDslNode.Term) {
            SearchDslNode.Term term = (SearchDslNode.Term) node;
            if (term.getField() != null) {
                return buildTerm(term, bind(term.getField()));
            }
            return combine(CompoundPredicate.Operator.OR,
                    fields.stream().map(field -> buildTerm(term, field)).collect(Collectors.toList()));
        }
        if (node instanceof SearchDslNode.Function) {
            SearchDslNode.Function function = (SearchDslNode.Function) node;
            if (function.getField() != null) {
                return buildFunction(function, bind(function.getField()));
            }
            return combine(CompoundPredicate.Operator.OR,
                    fields.stream().map(field -> buildFunction(function, field)).collect(Collectors.toList()));
        }
        if (node instanceof SearchDslNode.Implicit) {
            SearchDslNode.Implicit implicit = (SearchDslNode.Implicit) node;
            List<Expr> children = implicit.getChildren().stream()
                    .map(child -> buildCrossFields(child, fields)).collect(Collectors.toList());
            return combine(defaultCompoundOperator(), children);
        }
        if (node instanceof SearchDslNode.And) {
            List<Expr> children = ((SearchDslNode.And) node).getChildren().stream()
                    .map(child -> buildCrossFields(child, fields)).collect(Collectors.toList());
            return combine(CompoundPredicate.Operator.AND, children);
        }
        if (node instanceof SearchDslNode.Or) {
            List<Expr> children = ((SearchDslNode.Or) node).getChildren().stream()
                    .map(child -> buildCrossFields(child, fields)).collect(Collectors.toList());
            return combine(CompoundPredicate.Operator.OR, children);
        }
        if (node instanceof SearchDslNode.Not) {
            Expr child = buildCrossFields(((SearchDslNode.Not) node).getChild(), fields);
            return account(new CompoundPredicate(CompoundPredicate.Operator.NOT, child, null, position));
        }
        throw new IllegalStateException("unknown search DSL node: " + node.getClass().getSimpleName());
    }

    private FieldBinding bindingFor(String explicitField, FieldBinding fallback) {
        if (explicitField != null) {
            return bind(explicitField);
        }
        if (fallback == null) {
            throw new SemanticException("search() contains an unqualified clause; specify 'default_field' or 'fields'",
                    position);
        }
        return fallback;
    }

    private Expr buildTerm(SearchDslNode.Term term, FieldBinding field) {
        switch (term.getKind()) {
            case EXISTS:
                return account(new IsNullPredicate(slot(field), true, position));
            case WILDCARD:
                String wildcard = normalizeRawTerm(term.getWord(), field).replace('*', '%');
                return match(MatchExpr.MatchOperator.MATCH, field, wildcard);
            case TERM:
                return match(defaultMatchOperator(), field, term.getWord());
            default:
                throw new IllegalStateException("unknown search() term: " + term.getKind());
        }
    }

    private Expr buildFunction(SearchDslNode.Function function, FieldBinding field) {
        switch (function.getKind()) {
            case ANY:
                return match(MatchExpr.MatchOperator.MATCH_ANY, field, function.getQueryText());
            case ALL:
                return match(MatchExpr.MatchOperator.MATCH_ALL, field, function.getQueryText());
            case IN:
                List<Expr> alternatives = function.getWords().stream()
                        .map(word -> match(MatchExpr.MatchOperator.MATCH, field, normalizeRawTerm(word, field)))
                        .collect(Collectors.toList());
                return combine(CompoundPredicate.Operator.OR, alternatives);
            case EXACT:
                return match(MatchExpr.MatchOperator.MATCH, field,
                        normalizeRawTerm(function.getQueryText(), field));
            default:
                throw new IllegalStateException("unknown search() clause: " + function.getKind());
        }
    }

    private MatchExpr.MatchOperator defaultMatchOperator() {
        return options.getDefaultOperator() == SearchOptions.DefaultOperator.AND
                ? MatchExpr.MatchOperator.MATCH_ALL : MatchExpr.MatchOperator.MATCH_ANY;
    }

    private CompoundPredicate.Operator defaultCompoundOperator() {
        return options.getDefaultOperator() == SearchOptions.DefaultOperator.AND
                ? CompoundPredicate.Operator.AND : CompoundPredicate.Operator.OR;
    }

    private String normalizeRawTerm(String value, FieldBinding field) {
        return field.analyzerSettings.lowercaseRawTerms ? value.toLowerCase(Locale.ROOT) : value;
    }

    private Expr match(MatchExpr.MatchOperator operator, FieldBinding field, String query) {
        StringLiteral literal = account(new StringLiteral(query, position));
        return account(new MatchExpr(operator, slot(field), literal, position));
    }

    private SlotRef slot(FieldBinding field) {
        return account(new SlotRef(QualifiedName.of(field.referenceParts, position)));
    }

    private Expr combine(CompoundPredicate.Operator operator, List<Expr> children) {
        if (children.isEmpty()) {
            throw new IllegalStateException("cannot build an empty " + operator + " search predicate");
        }
        return combine(operator, children, 0, children.size());
    }

    private Expr combine(CompoundPredicate.Operator operator, List<Expr> children, int start, int end) {
        if (end - start == 1) {
            return children.get(start);
        }
        int middle = start + (end - start) / 2;
        Expr left = combine(operator, children, start, middle);
        Expr right = combine(operator, children, middle, end);
        return account(new CompoundPredicate(operator, left, right, position));
    }

    private <T extends Expr> T account(T expression) {
        expandedNodes++;
        if (expandedNodes > MAX_EXPANDED_NODES) {
            throw new SemanticException("search() expands to more than " + MAX_EXPANDED_NODES + " expression nodes",
                    position);
        }
        return expression;
    }

    private boolean containsUnqualifiedLeaf(SearchDslNode node) {
        if (node instanceof SearchDslNode.Term) {
            return ((SearchDslNode.Term) node).getField() == null;
        }
        if (node instanceof SearchDslNode.Function) {
            return ((SearchDslNode.Function) node).getField() == null;
        }
        if (node instanceof SearchDslNode.Not) {
            return containsUnqualifiedLeaf(((SearchDslNode.Not) node).getChild());
        }
        List<SearchDslNode> children;
        if (node instanceof SearchDslNode.Implicit) {
            children = ((SearchDslNode.Implicit) node).getChildren();
        } else if (node instanceof SearchDslNode.And) {
            children = ((SearchDslNode.And) node).getChildren();
        } else if (node instanceof SearchDslNode.Or) {
            children = ((SearchDslNode.Or) node).getChildren();
        } else {
            throw new IllegalStateException("unknown search DSL node: " + node.getClass().getSimpleName());
        }
        return children.stream().anyMatch(this::containsUnqualifiedLeaf);
    }

    private FieldBinding bind(String fieldName) {
        String cacheKey = fieldName.toLowerCase(Locale.ROOT);
        FieldBinding cached = bindingCache.get(cacheKey);
        if (cached != null) {
            return cached;
        }

        List<String> parts = Arrays.asList(fieldName.split("\\."));
        String columnName = parts.get(parts.size() - 1);
        validateQualifier(parts, fieldName);
        Column column = relationContext.resolveColumn(columnName, fieldName, position);
        if (!column.getType().isStringType()) {
            throw new SemanticException("search() field '" + fieldName + "' must be CHAR, VARCHAR, or STRING", position);
        }
        Index index = findGinIndex(column);
        if (index == null) {
            throw new SemanticException("search() field '" + fieldName + "' requires a GIN index", position);
        }

        FieldBinding binding = new FieldBinding(parts, column, GinAnalyzerSettings.from(index));
        bindingCache.put(cacheKey, binding);
        return binding;
    }

    private void validateQualifier(List<String> parts, String fieldName) {
        if (parts.size() == 1) {
            return;
        }
        if (parts.size() > 2) {
            throw new SemanticException("search() field '" + fieldName
                    + "' must use 'column' or 'table_alias.column'", position);
        }
        String qualifier = parts.get(parts.size() - 2);
        String visibleName = relationContext.getVisibleRelationName();
        if (!qualifier.equalsIgnoreCase(visibleName)) {
            throw new SemanticException("search() field '" + fieldName + "' does not belong to table '"
                    + visibleName + "'", position);
        }
    }

    private Index findGinIndex(Column column) {
        ColumnId columnId = column.getColumnId();
        for (Index index : table.getIndexes()) {
            if (index.getIndexType() == IndexDef.IndexType.GIN
                    && index.getColumns().stream().anyMatch(id -> id.equalsIgnoreCase(columnId))) {
                return index;
            }
        }
        return null;
    }

    private void validateCrossFieldAnalyzerSettings(List<FieldBinding> bindings) {
        GinAnalyzerSettings expected = bindings.get(0).analyzerSettings;
        for (int i = 1; i < bindings.size(); ++i) {
            if (!expected.equals(bindings.get(i).analyzerSettings)) {
                throw new SemanticException("search() type 'cross_fields' requires all fields to use compatible "
                        + "GIN parser, implementation, and case-normalization settings", position);
            }
        }
    }

    private static final class FieldBinding {
        private final List<String> referenceParts;
        private final Column column;
        private final GinAnalyzerSettings analyzerSettings;

        private FieldBinding(List<String> referenceParts, Column column, GinAnalyzerSettings analyzerSettings) {
            this.referenceParts = referenceParts;
            this.column = column;
            this.analyzerSettings = analyzerSettings;
        }
    }

    private static final class GinAnalyzerSettings {
        private final String implementation;
        private final String parser;
        private final boolean lowercaseRawTerms;

        private GinAnalyzerSettings(String implementation, String parser, boolean lowercaseRawTerms) {
            this.implementation = implementation;
            this.parser = parser;
            this.lowercaseRawTerms = lowercaseRawTerms;
        }

        private static GinAnalyzerSettings from(Index index) {
            Map<String, String> properties = index.getProperties();
            String implementation = valueOrDefault(
                    IndexAnalyzer.getPropertyIgnoreCase(properties, INVERTED_INDEX_IMP_LIB_KEY), "clucene");
            String parser = valueOrDefault(
                    IndexAnalyzer.getPropertyIgnoreCase(properties, INVERTED_INDEX_PARSER_KEY),
                    INVERTED_INDEX_PARSER_NONE);
            String lowerCase = IndexAnalyzer.getPropertyIgnoreCase(properties, INVERTED_INDEX_LOWER_CASE_KEY);
            boolean lowercaseRawTerms = !parser.equalsIgnoreCase(INVERTED_INDEX_PARSER_NONE)
                    && (!implementation.equalsIgnoreCase("builtin") || !"false".equalsIgnoreCase(lowerCase));
            return new GinAnalyzerSettings(implementation.toLowerCase(Locale.ROOT), parser.toLowerCase(Locale.ROOT),
                    lowercaseRawTerms);
        }

        private static String valueOrDefault(String value, String defaultValue) {
            return value == null ? defaultValue : value;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof GinAnalyzerSettings)) {
                return false;
            }
            GinAnalyzerSettings other = (GinAnalyzerSettings) object;
            return lowercaseRawTerms == other.lowercaseRawTerms
                    && implementation.equals(other.implementation) && parser.equals(other.parser);
        }

        @Override
        public int hashCode() {
            return Objects.hash(implementation, parser, lowercaseRawTerms);
        }
    }
}
