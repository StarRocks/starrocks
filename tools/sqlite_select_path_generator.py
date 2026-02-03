#!/usr/bin/env python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Generate SELECT statements that cover different grammar paths based on:
#   https://www.sqlite.org/syntax/select-stmt.html
#
# The generator is intentionally bounded to avoid combinatorial explosion.
# Use --max-outputs and --max-list-items to control the size of output.

import argparse
import random
import sys
from dataclasses import dataclass


class Node:
    def expand(self, ctx):
        raise NotImplementedError


@dataclass(frozen=True)
class Token(Node):
    value: str

    def expand(self, ctx):
        return [[self.value]]


class SeqNode(Node):
    def __init__(self, *nodes):
        self.nodes = list(nodes)

    def expand(self, ctx):
        expansions = [[]]
        for node in self.nodes:
            next_expansions = []
            for prefix in expansions:
                for suffix in node.expand(ctx):
                    next_expansions.append(prefix + suffix)
                    if len(next_expansions) >= ctx.node_limit:
                        break
                if len(next_expansions) >= ctx.node_limit:
                    break
            expansions = next_expansions
            if not expansions:
                break
        return expansions


class Choice(Node):
    def __init__(self, *options):
        self.options = list(options)

    def expand(self, ctx):
        expansions = []
        for opt in self.options:
            for variant in opt.expand(ctx):
                expansions.append(variant)
                if len(expansions) >= ctx.node_limit:
                    return expansions
        return expansions


class Optional(Node):
    def __init__(self, node):
        self.node = node

    def expand(self, ctx):
        expansions = self.node.expand(ctx)
        if len(expansions) < ctx.node_limit:
            expansions.append([])
        return expansions[:ctx.node_limit]


class Repeat(Node):
    def __init__(self, node, min_rep=0, max_rep=None, sep=None):
        self.node = node
        self.min_rep = min_rep
        self.max_rep = max_rep
        self.sep = sep

    def expand(self, ctx):
        max_rep = self.max_rep if self.max_rep is not None else ctx.max_list_items
        max_rep = max(self.min_rep, min(max_rep, ctx.max_list_items))
        lengths = [self.min_rep]
        if max_rep != self.min_rep:
            lengths = [max_rep, self.min_rep]

        expansions = []
        for length in lengths:
            expansions.extend(self._expand_length(length, ctx))
            if len(expansions) >= ctx.node_limit:
                break
        return expansions[:ctx.node_limit]

    def _expand_length(self, length, ctx):
        if length == 0:
            return [[]]
        nodes = []
        for i in range(length):
            if i > 0 and self.sep is not None:
                nodes.append(self.sep)
            nodes.append(self.node)
        return SeqNode(*nodes).expand(ctx)


@dataclass
class Context:
    node_limit: int
    max_list_items: int


def expr():
    return Choice(
        Token("1"),
        Token("c1"),
        SeqNode(Token("c1"), Token("+"), Token("1")),
        SeqNode(Token("c1"), Token("*"), Token("2")),
    )


def column_name():
    return Choice(Token("c1"), Token("c2"), Token("c3"))


def expr_list():
    return Repeat(expr(), min_rep=1, max_rep=None, sep=Token(","))


def result_column():
    return Choice(
        Token("*"),
        Token("c1"),
        Token("t1.c1"),
        SeqNode(Token("c1"), Token("AS"), Token("alias1")),
        SeqNode(Token("c1"), Token("+"), Token("1"), Token("AS"), Token("sum1")),
    )


def result_column_list():
    return Repeat(result_column(), min_rep=1, max_rep=None, sep=Token(","))


def ordering_term():
    return SeqNode(expr(), Optional(Choice(Token("ASC"), Token("DESC"))))


def ordering_term_list():
    return Repeat(ordering_term(), min_rep=1, max_rep=None, sep=Token(","))


def table_or_subquery():
    return Choice(
        Token("t1"),
        SeqNode(Token("t1"), Token("AS"), Token("a1")),
        SeqNode(Token("t1"), Token("INDEXED"), Token("BY"), Token("idx1")),
        SeqNode(Token("t1"), Token("NOT"), Token("INDEXED")),
        SeqNode(Token("("), Token("SELECT"), Token("1"), Token(")"), Token("AS"), Token("sub1")),
    )


def join_operator():
    return Choice(
        Token("JOIN"),
        SeqNode(Token("INNER"), Token("JOIN")),
        SeqNode(Token("LEFT"), Token("JOIN")),
        SeqNode(Token("LEFT"), Token("OUTER"), Token("JOIN")),
        SeqNode(Token("CROSS"), Token("JOIN")),
        SeqNode(Token("NATURAL"), Token("JOIN")),
    )


def join_constraint():
    return Choice(
        SeqNode(Token("ON"), expr()),
        SeqNode(
            Token("USING"),
            Token("("),
            Repeat(column_name(), min_rep=1, max_rep=None, sep=Token(",")),
            Token(")"),
        ),
    )


def join_clause():
    return SeqNode(
        table_or_subquery(),
        join_operator(),
        table_or_subquery(),
        Optional(join_constraint()),
    )


def from_clause():
    return SeqNode(
        Token("FROM"),
        Choice(
            Repeat(table_or_subquery(), min_rep=1, max_rep=None, sep=Token(",")),
            join_clause(),
        ),
    )


def where_clause():
    return SeqNode(Token("WHERE"), expr())


def group_by_clause():
    return SeqNode(
        Token("GROUP"),
        Token("BY"),
        expr_list(),
        Optional(SeqNode(Token("HAVING"), expr())),
    )


def window_def():
    return SeqNode(
        Choice(Token("w1"), Token("w2")),
        Token("AS"),
        Token("("),
        Optional(SeqNode(Token("PARTITION"), Token("BY"), expr_list())),
        Optional(SeqNode(Token("ORDER"), Token("BY"), ordering_term_list())),
        Token(")"),
    )


def window_clause():
    return SeqNode(
        Token("WINDOW"),
        Repeat(window_def(), min_rep=1, max_rep=2, sep=Token(",")),
    )


def set_quantifier():
    return Choice(Token("DISTINCT"), Token("ALL"))


def values_row():
    return SeqNode(Token("("), expr_list(), Token(")"))


def select_core():
    select_form = SeqNode(
        Token("SELECT"),
        Optional(set_quantifier()),
        result_column_list(),
        Optional(from_clause()),
        Optional(where_clause()),
        Optional(group_by_clause()),
        Optional(window_clause()),
    )
    values_form = SeqNode(
        Token("VALUES"),
        Repeat(values_row(), min_rep=1, max_rep=None, sep=Token(",")),
    )
    return Choice(select_form, values_form)


def compound_operator():
    return Choice(
        Token("UNION"),
        SeqNode(Token("UNION"), Token("ALL")),
        Token("INTERSECT"),
        Token("EXCEPT"),
    )


def compound_clause():
    return Repeat(
        SeqNode(compound_operator(), select_core()),
        min_rep=1,
        max_rep=1,
    )


def order_by_clause():
    return SeqNode(Token("ORDER"), Token("BY"), ordering_term_list())


def limit_expr():
    return Choice(Token("1"), Token("10"), Token("100"))


def limit_clause():
    return Choice(
        SeqNode(
            Token("LIMIT"),
            limit_expr(),
            Optional(SeqNode(Token("OFFSET"), limit_expr())),
        ),
        SeqNode(Token("LIMIT"), limit_expr(), Token(","), limit_expr()),
    )


def cte_def():
    return SeqNode(
        Choice(Token("cte1"), Token("cte2")),
        Token("AS"),
        Token("("),
        SeqNode(Token("SELECT"), Token("1")),
        Token(")"),
    )


def with_clause():
    return SeqNode(
        Token("WITH"),
        Optional(Token("RECURSIVE")),
        Repeat(cte_def(), min_rep=1, max_rep=2, sep=Token(",")),
    )


def select_stmt():
    return SeqNode(
        Optional(with_clause()),
        select_core(),
        Optional(compound_clause()),
        Optional(order_by_clause()),
        Optional(limit_clause()),
    )


def join_tokens(tokens):
    sql = " ".join(tokens)
    sql = sql.replace(" ,", ",")
    sql = sql.replace("( ", "(")
    sql = sql.replace(" )", ")")
    sql = sql.replace(" ;", ";")
    return sql.strip()


def dedupe(items):
    seen = set()
    result = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def generate_sql(max_outputs, max_list_items, shuffle, seed, with_semicolon):
    node_limit = min(max_outputs * 10, 5000)
    ctx = Context(node_limit=node_limit, max_list_items=max_list_items)
    expansions = select_stmt().expand(ctx)
    statements = [join_tokens(tokens) for tokens in expansions]
    statements = dedupe(statements)
    if shuffle:
        rng = random.Random(seed)
        rng.shuffle(statements)
    statements = statements[:max_outputs]
    if with_semicolon:
        statements = [f"{stmt};" for stmt in statements]
    return statements


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate SQLite SELECT statements across grammar paths."
    )
    parser.add_argument(
        "--max-outputs",
        type=int,
        default=200,
        help="Maximum number of SQL statements to emit.",
    )
    parser.add_argument(
        "--max-list-items",
        type=int,
        default=2,
        help="Maximum items for repeated lists (columns, tables, etc.).",
    )
    parser.add_argument(
        "--shuffle",
        action="store_true",
        help="Shuffle output order (use --seed for reproducibility).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=0,
        help="Random seed used with --shuffle.",
    )
    parser.add_argument(
        "--with-semicolon",
        action="store_true",
        help="Append a semicolon to each statement.",
    )
    parser.add_argument(
        "--output",
        help="Output file path (default: stdout).",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    statements = generate_sql(
        max_outputs=args.max_outputs,
        max_list_items=args.max_list_items,
        shuffle=args.shuffle,
        seed=args.seed,
        with_semicolon=args.with_semicolon,
    )
    output = sys.stdout
    if args.output:
        output = open(args.output, "w")
    for stmt in statements:
        output.write(stmt + "\n")
    if output is not sys.stdout:
        output.close()


if __name__ == "__main__":
    main()
