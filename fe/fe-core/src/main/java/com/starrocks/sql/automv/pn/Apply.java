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

package com.starrocks.sql.automv.pn;

import com.starrocks.type.Type;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

// Apply means expressions including builtin operators(e.g. + - * / %) or
// function invocation. Here we use Apply after Lisp concept in which, builtin
// operators and function invocation always adopt Polish-Notation and not distinguish
// builtin operators and function invocation, and both of them are treated as operator
// that is applied to its arguments.
public final class Apply extends Op {
    private final ApplyKind kind;
    private final boolean ordered;
    private final List<Op> args;

    public Apply(Type type, ApplyKind kind, boolean ordered, List<Op> args) {
        super(type);
        this.kind = kind;
        this.ordered = ordered;
        this.args = args;

    }

    public ApplyKind getKind() {
        return kind;
    }

    public boolean isOrdered() {
        return ordered;
    }

    @Override
    public Op clone() {
        return new Apply(this.getType(), this.getKind(), this.isOrdered(), this.getArgs());
    }

    @Override
    protected int hashCodeImpl() {
        return Objects.hash(type, kind.toString(), ordered, args);
    }

    @Override
    protected String toStringImpl() {
        String argsStr = args.stream().map(Op::toString).collect(Collectors.joining(" "));
        return String.format("(%s[%s][%s] %s)", kind.toString(), type.toSql(), ordered ? "O" : "U", argsStr);
    }

    @Override
    protected String toIsomorphicStringImpl() {
        String argsStr = args.stream().map(Op::toIsoMorphicString).collect(Collectors.joining(" "));
        return String.format("(%s[%s][%s] %s)", kind.toString(), type.toSql(), ordered ? "O" : "U", argsStr);
    }

    @Override
    protected SymTab getSymTabImpl() {
        if (!ordered) {
            return SymTab.of(args.stream().flatMap(arg -> arg.getSymTab().getEntries().stream())
                    .collect(Collectors.toList()));
        } else {
            Map<Boolean, List<Op>> argGroups = args.stream().collect(Collectors.partitioningBy(Op::isVar));
            List<Op> nonVarArgs = argGroups.get(false).stream().sorted(Comparator.comparing(Op::toString))
                    .collect(Collectors.toList());
            List<Op> varArgs = argGroups.get(true);

            List<SymTabEntry> entries =
                    nonVarArgs.stream().flatMap(var -> var.getSymTab().getEntries().stream())
                            .collect(Collectors.toList());
            if (!varArgs.isEmpty()) {
                entries.add(new SymbolSet(
                        varArgs.stream().flatMap(var -> var.getSymTab().getEntries().stream().map(e -> (Symbol) e))
                                .collect(
                                        Collectors.toSet())));
            }
            return SymTab.of(entries);
        }
    }

    @Override
    public <R, C> R accept(OpVisitor<R, C> visitor, C context) {
        return visitor.visitApply(this, context);
    }

    @Override
    public int nary() {
        return getSymTab().getEntries().stream()
                .map(e -> e instanceof SymbolSet ? ((SymbolSet) e).getEntries().size() : 1)
                .reduce(0, Integer::sum);
    }

    @Override
    protected int getHeightImpl() {
        return 1 + args.stream().map(Op::getHeight).reduce(0, Math::max);
    }

    public List<Op> getArgs() {
        return Collections.unmodifiableList(args);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Apply apply = (Apply) o;
        if (hashCode() != o.hashCode()) {
            return false;
        }
        if (getSymTab().getEntries().size() != apply.getSymTab().getEntries().size()) {
            return false;
        }
        return toString().equals(apply.toString());
    }
}