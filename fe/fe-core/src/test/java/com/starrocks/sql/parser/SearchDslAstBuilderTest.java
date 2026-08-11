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

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SearchDslAstBuilderTest {
    @Test
    public void testConventionalBooleanPrecedence() {
        SearchDslNode.Or root = assertInstanceOf(SearchDslNode.Or.class, parse("a OR b AND c"));
        assertEquals(2, root.getChildren().size());
        assertInstanceOf(SearchDslNode.Term.class, root.getChildren().get(0));
        assertInstanceOf(SearchDslNode.And.class, root.getChildren().get(1));

        SearchDslNode.Or second = assertInstanceOf(SearchDslNode.Or.class, parse("a AND b OR c"));
        assertInstanceOf(SearchDslNode.And.class, second.getChildren().get(0));
        assertInstanceOf(SearchDslNode.Term.class, second.getChildren().get(1));
    }

    @Test
    public void testImplicitClausesPreserveExplicitOperatorPrecedence() {
        SearchDslNode.Or root = assertInstanceOf(SearchDslNode.Or.class, parse("a b AND c d OR e"));
        SearchDslNode.And and = assertInstanceOf(SearchDslNode.And.class, root.getChildren().get(0));
        SearchDslNode.Implicit left = assertInstanceOf(SearchDslNode.Implicit.class, and.getChildren().get(0));
        SearchDslNode.Implicit right = assertInstanceOf(SearchDslNode.Implicit.class, and.getChildren().get(1));
        assertEquals(2, left.getChildren().size());
        assertEquals(2, right.getChildren().size());
        assertInstanceOf(SearchDslNode.Term.class, root.getChildren().get(1));
    }

    @Test
    public void testParenthesesOverridePrecedence() {
        SearchDslNode.And root = assertInstanceOf(SearchDslNode.And.class, parse("(a OR b) AND c"));
        assertInstanceOf(SearchDslNode.Or.class, root.getChildren().get(0));
    }

    @Test
    public void testNotPrecedenceAndChain() {
        SearchDslNode.And root = assertInstanceOf(SearchDslNode.And.class, parse("NOT NOT a AND b"));
        SearchDslNode.Not first = assertInstanceOf(SearchDslNode.Not.class, root.getChildren().get(0));
        assertInstanceOf(SearchDslNode.Not.class, first.getChild());
    }

    @Test
    public void testAdjacentClausesFormImplicitNode() {
        SearchDslNode.Implicit implicit = assertInstanceOf(SearchDslNode.Implicit.class, parse("a b NOT c"));
        assertEquals(3, implicit.getChildren().size());
        assertInstanceOf(SearchDslNode.Not.class, implicit.getChildren().get(2));

        ParsingException adjacent = assertThrows(ParsingException.class, () -> parse("a(b)"));
        assertTrue(adjacent.getDetailMsg().contains("whitespace starts a separate clause"));

        ParsingException afterFunction =
                assertThrows(ParsingException.class, () -> parse("f:EXACT(foo)bar"));
        assertTrue(afterFunction.getDetailMsg().contains("does not extend the preceding term"));

        SearchDslNode.Implicit supplementary =
                assertInstanceOf(SearchDslNode.Implicit.class, parse("😀 𠮷"));
        assertEquals(2, supplementary.getChildren().size());
    }

    @Test
    public void testFieldGroupAndMixedQualifiedTerms() {
        SearchDslNode.And root = assertInstanceOf(SearchDslNode.And.class,
                parse("title:(foo OR bar) AND category:database"));
        SearchDslNode.Or title = assertInstanceOf(SearchDslNode.Or.class, root.getChildren().get(0));
        for (SearchDslNode child : title.getChildren()) {
            assertEquals("title", assertInstanceOf(SearchDslNode.Term.class, child).getField());
        }
        assertEquals("category", assertInstanceOf(SearchDslNode.Term.class, root.getChildren().get(1)).getField());

        SearchDslNode.Or override = assertInstanceOf(SearchDslNode.Or.class,
                parse("title:(foo OR body:bar OR baz)"));
        assertEquals("title", assertInstanceOf(SearchDslNode.Term.class, override.getChildren().get(0)).getField());
        assertEquals("body", assertInstanceOf(SearchDslNode.Term.class, override.getChildren().get(1)).getField());
        assertEquals("title", assertInstanceOf(SearchDslNode.Term.class, override.getChildren().get(2)).getField());

        SearchDslNode.Implicit implicit = assertInstanceOf(SearchDslNode.Implicit.class, parse("title:(foo bar)"));
        for (SearchDslNode child : implicit.getChildren()) {
            assertEquals("title", assertInstanceOf(SearchDslNode.Term.class, child).getField());
        }
    }

    @Test
    public void testSearchClauses() {
        SearchDslNode.Function any = assertInstanceOf(SearchDslNode.Function.class, parse("f:ANY(foo bar)"));
        assertEquals(SearchDslNode.Function.Kind.ANY, any.getKind());
        assertEquals(2, any.getWords().size());

        SearchDslNode.Function exact = assertInstanceOf(
                SearchDslNode.Function.class, parse("EXACT(foo  \tbar)"));
        assertEquals(SearchDslNode.Function.Kind.EXACT, exact.getKind());
        assertEquals("foo  \tbar", exact.getQueryText());

        SearchDslNode.Function keywordArguments =
                assertInstanceOf(SearchDslNode.Function.class, parse("f:ANY(any all in exact)"));
        assertEquals(4, keywordArguments.getWords().size());
        assertEquals("all", keywordArguments.getWords().get(1));
    }

    @Test
    public void testClauseNamesRemainUsableAsTermsAndFields() {
        SearchDslNode.Term term = assertInstanceOf(SearchDslNode.Term.class, parse("any"));
        assertEquals("any", term.getWord());

        SearchDslNode.Term qualified = assertInstanceOf(SearchDslNode.Term.class, parse("all:exact"));
        assertEquals("all", qualified.getField());
        assertEquals("exact", qualified.getWord());

        assertEquals("01", assertInstanceOf(SearchDslNode.Term.class, parse("01:value")).getField());
    }

    @Test
    public void testWhitespaceBeforeClauseParenthesisIsRejected() {
        ParsingException exception = assertThrows(ParsingException.class, () -> parse("f:ANY (foo)"));
        assertTrue(exception.getMessage().contains("immediately followed"));
    }

    @Test
    public void testUnicodeWhitespaceOnlyDslIsEmpty() {
        ParsingException exception = assertThrows(ParsingException.class, () -> parse("\u3000"));
        assertTrue(exception.getMessage().contains("search() DSL must not be empty"));
    }

    @Test
    public void testUnsupportedEsSyntaxIsRejected() {
        assertThrows(ParsingException.class, () -> parse("title:foo^2"));
        assertThrows(ParsingException.class, () -> parse("+foo AND -bar"));
        assertThrows(ParsingException.class, () -> parse("foo && bar"));
        assertThrows(ParsingException.class, () -> parse("title:Nested"));
        assertThrows(ParsingException.class, () -> parse("title:foo?"));
    }

    @Test
    public void testWildcardAndExistsRemainDistinct() {
        SearchDslNode.Term term = assertInstanceOf(SearchDslNode.Term.class, parse("f:foo"));
        assertEquals(SearchDslNode.Term.Kind.TERM, term.getKind());

        SearchDslNode.Term wildcard = assertInstanceOf(SearchDslNode.Term.class, parse("f:foo*"));
        assertEquals(SearchDslNode.Term.Kind.WILDCARD, wildcard.getKind());

        SearchDslNode.Term exists = assertInstanceOf(SearchDslNode.Term.class, parse("f:*"));
        assertEquals(SearchDslNode.Term.Kind.EXISTS, exists.getKind());

        SearchDslNode.Term unicode = assertInstanceOf(SearchDslNode.Term.class, parse("title:机器学习"));
        assertEquals("机器学习", unicode.getWord());

        assertThrows(ParsingException.class, () -> parse("f:*foo"));
        assertThrows(ParsingException.class, () -> parse("f:fo*o"));
        assertThrows(ParsingException.class, () -> parse("f:foo**"));
    }

    @Test
    public void testNestingBoundaries() {
        assertNotNull(parse("(".repeat(200) + "a" + ")".repeat(200)));
        assertThrows(ParsingException.class,
                () -> parse("(".repeat(201) + "a" + ")".repeat(201)));

        assertNotNull(parse("NOT ".repeat(200) + "a"));
        assertThrows(ParsingException.class, () -> parse("NOT ".repeat(201) + "a"));
    }

    @Test
    public void testInputLengthBoundaries() {
        assertNotNull(parse("a".repeat(1 << 20)));
        assertThrows(ParsingException.class, () -> parse("a".repeat((1 << 20) + 1)));

        assertNotNull(SearchOptions.parse("{}" + " ".repeat(4094), NodePosition.ZERO));
        assertThrows(ParsingException.class,
                () -> SearchOptions.parse("{}" + " ".repeat(4095), NodePosition.ZERO));
    }

    @Test
    public void testMalformedClausesAreRejectedByGrammar() {
        assertThrows(ParsingException.class, () -> parse("f:ANY()"));
        assertThrows(ParsingException.class, () -> parse("f:ANY(foo"));
        assertThrows(ParsingException.class, () -> parse("f:ANY(foo:bar)"));
        assertThrows(ParsingException.class, () -> parse("f:ANY(foo AND bar)"));
        assertThrows(ParsingException.class, () -> parse("f:ANY((foo))"));
        assertThrows(ParsingException.class, () -> parse("f:IN(foo*)"));
    }

    @Test
    public void testOptionsValidation() {
        SearchOptions defaultField = SearchOptions.parse("{\"default_field\":\"body\"}", NodePosition.ZERO);
        assertEquals(List.of("body"), defaultField.getEffectiveFields());

        SearchOptions singleField = SearchOptions.parse("{\"fields\":[\"body\"]}", NodePosition.ZERO);
        assertEquals(defaultField.getEffectiveFields(), singleField.getEffectiveFields());
        assertTrue(SearchOptions.defaults().getEffectiveFields().isEmpty());

        assertThrows(ParsingException.class,
                () -> SearchOptions.parse(
                        "{\"default_field\":\"body\",\"fields\":[\"title\"]}", NodePosition.ZERO));
        assertThrows(ParsingException.class,
                () -> SearchOptions.parse("{\"fields\":[]}", NodePosition.ZERO));
        assertThrows(ParsingException.class,
                () -> SearchOptions.parse("{\"type\":\"cross_fields\"}", NodePosition.ZERO));
        assertThrows(ParsingException.class,
                () -> SearchOptions.parse("{\"fields\":[\"a\",\"A\"]}", NodePosition.ZERO));
        assertThrows(ParsingException.class,
                () -> SearchOptions.parse("{\"default_field\":\"a\",\"default_field\":\"b\"}",
                        NodePosition.ZERO));
        assertThrows(ParsingException.class,
                () -> SearchOptions.parse("{\"default_operator\":\"xor\"}", NodePosition.ZERO));
        assertThrows(ParsingException.class,
                () -> SearchOptions.parse("{\"unknown\":true}", NodePosition.ZERO));
        assertThrows(ParsingException.class,
                () -> SearchOptions.parse("{\"fields\":\"a\"}", NodePosition.ZERO));
        assertThrows(ParsingException.class,
                () -> SearchOptions.parse("{\"fields\":[\"a-b\"]}", NodePosition.ZERO));
        assertThrows(ParsingException.class,
                () -> SearchOptions.parse("{\"default_field\":\"a\",}", NodePosition.ZERO));
    }

    private static SearchDslNode parse(String dsl) {
        return SearchDslAstBuilder.parse(dsl, NodePosition.ZERO);
    }
}
