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

import org.apache.commons.collections.Unmodifiable;

import java.util.Collections;
import java.util.Comparator;
import java.util.Set;
import java.util.stream.Collectors;

public final class SymbolSet extends SymTabEntry {
    private final Set<Symbol> entries;

    SymbolSet(Set<Symbol> entries) {

        this.entries = (entries instanceof Unmodifiable) ? entries : Collections.unmodifiableSet(entries);
    }

    public Set<Symbol> getEntries() {
        return entries;
    }

    @Override
    public int hashCodeImpl() {
        return entries.hashCode();
    }

    @Override
    protected String toStringImpl() {
        String s = entries.stream().sorted(Comparator.comparingInt(e -> e.id)).map(e -> "" + e.id)
                .collect(Collectors.joining(","));
        return String.format("SymbolSet{%s}", s);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SymbolSet symbolSet = (SymbolSet) o;
        return symbolSet.hashCode() == this.hashCode() && symbolSet.toString().equals(this.toString());
    }
}
