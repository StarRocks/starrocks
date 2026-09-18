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

// Lexer for the DSL string accepted by search('<dsl>' [, '<options-json>']).
// Business semantics such as field binding and GIN-index validation deliberately
// stay outside this grammar and run after the containing SQL query has a table scope.
lexer grammar SearchDslLexer;

LPAREN: '(';
RPAREN: ')';
COLON: ':';

// Boolean operators follow Elasticsearch's spelling rule: only uppercase forms
// are operators. Lowercase and/or/not remain ordinary terms.
AND: 'AND';
OR: 'OR';
NOT: 'NOT';

// Search clause names are case-insensitive. The parser only treats one as a
// clause when it is immediately followed by '('.
ANY: A N Y;
ALL: A L L;
IN: I N;
EXACT: E X A C T;

// Match Java Character.isWhitespace except for the three non-breaking spaces.
WS: [\u0009-\u000D\u001C-\u001F\u0020\u1680\u2000-\u2006\u2008-\u200A\u2028\u2029\u205F\u3000]+ -> skip;

WORD: ~[():\u0009-\u000D\u001C-\u001F\u0020\u1680\u2000-\u2006\u2008-\u200A\u2028\u2029\u205F\u3000]+;

fragment A: [aA];
fragment L: [lL];
fragment N: [nN];
fragment Y: [yY];
fragment I: [iI];
fragment E: [eE];
fragment X: [xX];
fragment C: [cC];
fragment T: [tT];
