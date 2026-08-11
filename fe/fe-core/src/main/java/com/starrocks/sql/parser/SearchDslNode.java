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

package com.starrocks.sql.parser;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Metadata-independent AST for the string DSL accepted by {@code search()}.
 *
 * <p>The parser records explicit fields, explicit Boolean operators, and whitespace-separated
 * implicit clauses, but does not resolve columns, inspect indexes, or choose StarRocks MATCH
 * operators. Those operations require the query scope and are performed by the analyzer after
 * this AST has been built.</p>
 */
public abstract class SearchDslNode {
    public static final class Term extends SearchDslNode {
        public enum Kind {
            TERM,
            WILDCARD,
            EXISTS
        }

        private final String field;
        private final String word;
        private final Kind kind;

        Term(String field, String word, Kind kind) {
            this.field = field;
            this.word = word;
            this.kind = kind;
        }

        public String getField() {
            return field;
        }

        public String getWord() {
            return word;
        }

        public Kind getKind() {
            return kind;
        }
    }

    public static final class Function extends SearchDslNode {
        public enum Kind {
            ANY,
            ALL,
            IN,
            EXACT
        }

        private final String field;
        private final Kind kind;
        private final List<String> words;
        private final String queryText;

        Function(String field, Kind kind, List<String> words, String queryText) {
            this.field = field;
            this.kind = kind;
            this.words = ImmutableList.copyOf(words);
            this.queryText = queryText;
        }

        public String getField() {
            return field;
        }

        public Kind getKind() {
            return kind;
        }

        public List<String> getWords() {
            return words;
        }

        /** Source text from the first through last argument, preserving internal whitespace. */
        public String getQueryText() {
            return queryText;
        }
    }

    /** Clauses separated only by DSL whitespace; the analyzer applies default_operator later. */
    public static final class Implicit extends SearchDslNode {
        private final List<SearchDslNode> children;

        Implicit(List<SearchDslNode> children) {
            this.children = ImmutableList.copyOf(children);
        }

        public List<SearchDslNode> getChildren() {
            return children;
        }
    }

    public static final class And extends SearchDslNode {
        private final List<SearchDslNode> children;

        And(List<SearchDslNode> children) {
            this.children = ImmutableList.copyOf(children);
        }

        public List<SearchDslNode> getChildren() {
            return children;
        }
    }

    public static final class Or extends SearchDslNode {
        private final List<SearchDslNode> children;

        Or(List<SearchDslNode> children) {
            this.children = ImmutableList.copyOf(children);
        }

        public List<SearchDslNode> getChildren() {
            return children;
        }
    }

    public static final class Not extends SearchDslNode {
        private final SearchDslNode child;

        Not(SearchDslNode child) {
            this.child = child;
        }

        public SearchDslNode getChild() {
            return child;
        }
    }
}
