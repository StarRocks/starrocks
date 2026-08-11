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

// Parser for search()'s standard-mode DSL. Explicit operators use the conventional
// precedence NOT > AND > OR; parentheses override it. Whitespace-separated clauses
// are kept as an implicitClause so the analyzer can apply default_operator after it
// knows how fields will be bound.
parser grammar SearchDslParser;

options { tokenVocab=SearchDslLexer; }

query
    : orClause EOF
    ;

orClause
    : andClause (OR andClause)*
    ;

andClause
    : implicitClause (AND implicitClause)*
    ;

implicitClause
    // WS is skipped by the lexer; the AST builder verifies an actual whitespace
    // gap between consecutive unary clauses so forms such as foo(bar) stay invalid.
    : unaryClause+
    ;

// A loop avoids recursive-descent stack growth for a long NOT chain. The AST
// builder applies a separate nesting budget before constructing the nodes.
unaryClause
    : NOT* primary
    ;

primary
    : fieldClause
    | funcCall
    | LPAREN orClause RPAREN
    | word
    ;

fieldClause
    : fieldName COLON fieldValue
    ;

fieldName
    : WORD | ANY | ALL | IN | EXACT
    ;

fieldValue
    : funcCall
    | LPAREN orClause RPAREN
    | word
    ;

funcCall
    : funcName LPAREN funcArg+ RPAREN
    ;

funcName
    : ANY | ALL | IN | EXACT
    ;

funcArg
    : WORD | ANY | ALL | IN | EXACT
    ;

word
    : WORD | ANY | ALL | IN | EXACT
    ;
