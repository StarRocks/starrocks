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

import com.starrocks.common.util.UnionFind;
import org.apache.commons.collections.Unmodifiable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SymTab {
    public static final SymTab EMPTY = new SymTab(Collections.emptyList());
    private final List<SymTabEntry> entries;
    private transient String cachedString = null;
    private transient Integer cachedHashCode = null;

    private SymTab(List<SymTabEntry> entries) {
        this.entries = (entries instanceof Unmodifiable) ? entries : Collections.unmodifiableList(entries);
    }

    public static SymTab of(List<SymTabEntry> entries) {
        return entries.isEmpty() ? EMPTY : new SymTab(entries);
    }

    public static boolean equivalent(Op a, Op b, UnionFind<Integer> equivSymbols) {
        List<SymTabEntry> aSymTab = a.getSymTab().getEntries();
        List<SymTabEntry> bSymTab = b.getSymTab().getEntries();
        boolean symTabNotMatched = IntStream.range(0, aSymTab.size())
                .anyMatch(i -> !aSymTab.get(i).getClass().equals(bSymTab.get(i).getClass()));
        if (symTabNotMatched) {
            return false;
        }

        for (int i = 0; i < aSymTab.size(); ++i) {
            SymTabEntry aSym = aSymTab.get(i);
            SymTabEntry bSym = bSymTab.get(i);
            if (aSym.equals(bSym)) {
                continue;
            }
            if (aSym instanceof Symbol) {
                Symbol sa = (Symbol) aSym;
                Symbol sb = (Symbol) bSym;
                Optional<Integer> ga = equivSymbols.getGroupId(sa.getId());
                Optional<Integer> gb = equivSymbols.getGroupId(sb.getId());
                if (!ga.isPresent() || !gb.isPresent() || !ga.get().equals(gb.get())) {
                    return false;
                }
            } else {
                SymbolSet ssa = (SymbolSet) aSym;
                SymbolSet ssb = (SymbolSet) bSym;
                Set<Integer> gsa =
                        ssa.getEntries().stream().map(e -> equivSymbols.getGroupId(e.getId()).orElse(-e.getId()))
                                .collect(Collectors.toSet());
                Set<Integer> gsb =
                        ssb.getEntries().stream().map(e -> equivSymbols.getGroupId(e.getId()).orElse(-e.getId()))
                                .collect(Collectors.toSet());
                if (!gsa.equals(gsb)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public String toString() {
        if (cachedString == null) {
            cachedString = String.format("SymbolTable[%s]", entries.stream().map(SymTabEntry::toString).collect(
                    Collectors.joining(", ")));
        }
        return cachedString;
    }

    public List<SymTabEntry> getEntries() {
        return entries;
    }

    @Override
    public int hashCode() {
        if (cachedHashCode == null) {
            cachedHashCode = Objects.hash(entries);
        }
        return cachedHashCode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SymTab symTab = (SymTab) o;
        if (this.entries.size() != symTab.entries.size() || this.hashCode() != symTab.hashCode()) {
            return false;
        }
        return Objects.equals(entries, symTab.entries);
    }
}
