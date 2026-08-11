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

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.InputMismatchException;
import org.antlr.v4.runtime.NoViableAltException;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/** Converts the standalone search DSL parse tree into a metadata-independent {@link SearchDslNode}. */
public final class SearchDslAstBuilder extends SearchDslParserBaseVisitor<SearchDslNode> {
    private static final Pattern FIELD_NAME_PATTERN =
            Pattern.compile("[A-Za-z0-9_]+(\\.[A-Za-z0-9_]+)*");
    private static final int MAX_NESTING_DEPTH = 200;
    private static final int MAX_DSL_LENGTH = 1 << 20;

    private final NodePosition literalPosition;
    private String inheritedField;
    private int depth;

    private SearchDslAstBuilder(NodePosition literalPosition) {
        this.literalPosition = literalPosition;
    }

    public static boolean isValidFieldName(String fieldName) {
        return fieldName != null && FIELD_NAME_PATTERN.matcher(fieldName).matches();
    }

    public static SearchDslNode parse(String dsl, NodePosition literalPosition) {
        if (dsl == null || dsl.isBlank()) {
            throw new ParsingException("search() DSL must not be empty", literalPosition);
        }
        if (dsl.length() > MAX_DSL_LENGTH) {
            throw new ParsingException("search() DSL is too long, at most " + MAX_DSL_LENGTH
                    + " characters are supported", literalPosition);
        }

        SearchDslLexer lexer = new SearchDslLexer(CharStreams.fromString(dsl));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        tokens.fill();
        checkParenthesisDepth(tokens.getTokens(), literalPosition);

        SearchDslParser parser = new SearchDslParser(tokens);
        parser.removeErrorListeners();
        parser.setErrorHandler(new ThrowingErrorStrategy(literalPosition));
        SearchDslParser.QueryContext query = parser.query();
        return new SearchDslAstBuilder(literalPosition).visit(query);
    }

    private static void checkParenthesisDepth(List<Token> tokens, NodePosition position) {
        int nesting = 0;
        for (Token token : tokens) {
            if (token.getType() == SearchDslLexer.LPAREN) {
                nesting++;
                if (nesting > MAX_NESTING_DEPTH) {
                    throw new ParsingException(nestingDepthMessage(), position);
                }
            } else if (token.getType() == SearchDslLexer.RPAREN && nesting > 0) {
                nesting--;
            }
        }
    }

    private static String nestingDepthMessage() {
        return "search() DSL nesting is too deep, at most " + MAX_NESTING_DEPTH + " levels are supported";
    }

    @Override
    public SearchDslNode visitQuery(SearchDslParser.QueryContext context) {
        return visit(context.orClause());
    }

    @Override
    public SearchDslNode visitOrClause(SearchDslParser.OrClauseContext context) {
        List<SearchDslNode> children = new ArrayList<>();
        for (SearchDslParser.AndClauseContext child : context.andClause()) {
            children.add(visit(child));
        }
        if (children.size() == 1) {
            return children.get(0);
        }
        return new SearchDslNode.Or(children);
    }

    @Override
    public SearchDslNode visitAndClause(SearchDslParser.AndClauseContext context) {
        List<SearchDslNode> children = new ArrayList<>();
        for (SearchDslParser.ImplicitClauseContext child : context.implicitClause()) {
            children.add(visit(child));
        }
        if (children.size() == 1) {
            return children.get(0);
        }
        return new SearchDslNode.And(children);
    }

    @Override
    public SearchDslNode visitImplicitClause(SearchDslParser.ImplicitClauseContext context) {
        List<SearchDslParser.UnaryClauseContext> clauses = context.unaryClause();
        List<SearchDslNode> children = new ArrayList<>();
        for (int i = 0; i < clauses.size(); ++i) {
            if (i > 0) {
                requireWhitespaceBetween(clauses.get(i - 1), clauses.get(i));
            }
            children.add(visit(clauses.get(i)));
        }
        if (children.size() == 1) {
            return children.get(0);
        }
        return new SearchDslNode.Implicit(children);
    }

    private void requireWhitespaceBetween(SearchDslParser.UnaryClauseContext left,
                                          SearchDslParser.UnaryClauseContext right) {
        int start = left.getStop().getStopIndex() + 1;
        int end = right.getStart().getStartIndex();
        if (start >= end || !sourceText(left.getStop(), start, end - 1).codePoints()
                .allMatch(Character::isWhitespace)) {
            throw error("unexpected adjacent search() clauses; whitespace starts a separate clause "
                    + "and does not extend the preceding term");
        }
    }

    @Override
    public SearchDslNode visitUnaryClause(SearchDslParser.UnaryClauseContext context) {
        List<TerminalNode> nots = context.NOT();
        depth += nots.size();
        if (depth > MAX_NESTING_DEPTH) {
            throw error(nestingDepthMessage());
        }
        SearchDslNode node;
        try {
            node = visit(context.primary());
        } finally {
            depth -= nots.size();
        }
        for (int i = nots.size() - 1; i >= 0; --i) {
            node = new SearchDslNode.Not(node);
        }
        return node;
    }

    @Override
    public SearchDslNode visitPrimary(SearchDslParser.PrimaryContext context) {
        if (context.fieldClause() != null) {
            return visit(context.fieldClause());
        }
        if (context.funcCall() != null) {
            return buildFunction(context.funcCall(), inheritedField);
        }
        if (context.LPAREN() != null) {
            return visitGroup(context.orClause(), inheritedField);
        }
        return buildTerm(inheritedField, context.word());
    }

    @Override
    public SearchDslNode visitFieldClause(SearchDslParser.FieldClauseContext context) {
        Token fieldToken = context.fieldName().getStart();
        String field = fieldToken.getText();
        if (!isValidFieldName(field)) {
            throw error("invalid field name '" + field + "' in search()");
        }

        SearchDslParser.FieldValueContext value = context.fieldValue();
        if (value.funcCall() != null) {
            return buildFunction(value.funcCall(), field);
        }
        if (value.LPAREN() != null) {
            return visitGroup(value.orClause(), field);
        }
        return buildTerm(field, value.word());
    }

    private SearchDslNode visitGroup(SearchDslParser.OrClauseContext context, String field) {
        enterGroup();
        String previousField = inheritedField;
        inheritedField = field;
        try {
            return visit(context);
        } finally {
            inheritedField = previousField;
            depth--;
        }
    }

    private void enterGroup() {
        depth++;
        if (depth > MAX_NESTING_DEPTH) {
            throw error(nestingDepthMessage());
        }
    }

    private SearchDslNode buildFunction(SearchDslParser.FuncCallContext context, String field) {
        SearchDslNode.Function.Kind kind = functionKind(context);
        Token name = context.funcName().getStart();
        Token leftParenthesis = context.LPAREN().getSymbol();
        if (name.getStopIndex() + 1 != leftParenthesis.getStartIndex()) {
            throw error(kind + " must be immediately followed by '('");
        }

        List<String> words = new ArrayList<>();
        for (SearchDslParser.FuncArgContext argument : context.funcArg()) {
            Token token = argument.getStart();
            validateWord(token.getText());
            if (token.getText().indexOf('*') >= 0) {
                throw error("wildcard '*' is not supported inside " + kind + "(...): '"
                        + token.getText() + "'");
            }
            words.add(token.getText());
        }
        Token firstArgument = context.funcArg(0).getStart();
        Token lastArgument = context.funcArg(context.funcArg().size() - 1).getStop();
        String queryText = sourceText(firstArgument, firstArgument.getStartIndex(), lastArgument.getStopIndex());
        return new SearchDslNode.Function(field, kind, words, queryText);
    }

    private static SearchDslNode.Function.Kind functionKind(SearchDslParser.FuncCallContext context) {
        switch (context.funcName().getStart().getType()) {
            case SearchDslLexer.ANY:
                return SearchDslNode.Function.Kind.ANY;
            case SearchDslLexer.ALL:
                return SearchDslNode.Function.Kind.ALL;
            case SearchDslLexer.IN:
                return SearchDslNode.Function.Kind.IN;
            case SearchDslLexer.EXACT:
                return SearchDslNode.Function.Kind.EXACT;
            default:
                throw new IllegalStateException("unexpected search() clause: " + context.funcName().getText());
        }
    }

    private SearchDslNode buildTerm(String field, SearchDslParser.WordContext context) {
        Token token = context.getStart();
        String word = token.getText();
        validateWord(word);
        if (word.equals("*")) {
            return new SearchDslNode.Term(field, null, SearchDslNode.Term.Kind.EXISTS);
        }
        int wildcard = word.indexOf('*');
        if (wildcard >= 0 && (wildcard != word.length() - 1 || word.lastIndexOf('*') != wildcard)) {
            throw error("wildcard '*' is only supported once at the end of a search() term: '" + word + "'");
        }
        SearchDslNode.Term.Kind kind = wildcard >= 0 ? SearchDslNode.Term.Kind.WILDCARD : SearchDslNode.Term.Kind.TERM;
        return new SearchDslNode.Term(field, word, kind);
    }

    private static String sourceText(Token token, int start, int stop) {
        CharStream input = token.getInputStream();
        return input.getText(Interval.of(start, stop));
    }

    private void validateWord(String word) {
        String message = invalidWordMessage(word);
        if (message != null) {
            throw error(message);
        }
    }

    private static String invalidWordMessage(String word) {
        if (word.indexOf('"') >= 0) {
            return "phrase query is not supported in search(): '" + word + "'";
        }
        if (word.startsWith("/")) {
            return "regex query is not supported in search(): '" + word + "'";
        }
        if (word.indexOf('[') >= 0 || word.indexOf('{') >= 0 || word.startsWith(">") || word.startsWith("<")) {
            return "range query is not supported in search(): '" + word + "'";
        }
        if (word.indexOf('~') >= 0) {
            return "fuzzy query is not supported in search(): '" + word + "'";
        }
        if (word.indexOf('?') >= 0) {
            return "single-character wildcard '?' is not supported in search(), use '*' instead: '" + word + "'";
        }
        if (word.indexOf('%') >= 0) {
            return "literal '%' is not allowed in search() terms: '" + word + "'";
        }
        if (word.indexOf('\\') >= 0) {
            return "escape character is not supported in search(): '" + word + "'";
        }
        if (word.indexOf('^') >= 0) {
            return "boost query is not supported in search(): '" + word + "'";
        }
        if (word.contains("&&") || word.contains("||") || word.indexOf('!') >= 0) {
            return "use the uppercase AND, OR, and NOT operators in search(): '" + word + "'";
        }
        if ((word.startsWith("+") || word.startsWith("-")) && word.length() > 1) {
            return "required/prohibited term prefixes are not supported in search(): '" + word + "'";
        }
        if (word.toUpperCase(Locale.ROOT).equals("NESTED")) {
            return "NESTED is not supported in search()";
        }
        return null;
    }

    private ParsingException error(String message) {
        return new ParsingException(message, literalPosition);
    }

    private static final class ThrowingErrorStrategy extends DefaultErrorStrategy {
        private final NodePosition position;

        private ThrowingErrorStrategy(NodePosition position) {
            this.position = position;
        }

        @Override
        public void reportInputMismatch(Parser recognizer, InputMismatchException exception) {
            fail(exception.getOffendingToken());
        }

        @Override
        public void reportNoViableAlternative(Parser recognizer, NoViableAltException exception) {
            fail(exception.getOffendingToken());
        }

        @Override
        public void reportMissingToken(Parser recognizer) {
            fail(recognizer.getCurrentToken());
        }

        @Override
        public void reportUnwantedToken(Parser recognizer) {
            fail(recognizer.getCurrentToken());
        }

        @Override
        public void reportError(Parser recognizer, RecognitionException exception) {
            fail(exception.getOffendingToken());
        }

        private void fail(Token token) {
            String found = token == null || token.getType() == Token.EOF
                    ? "end of input" : "'" + token.getText() + "'";
            throw new ParsingException("unexpected " + found + " in search() DSL", position);
        }
    }
}
