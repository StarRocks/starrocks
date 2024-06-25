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

package com.starrocks.sql.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.DoNotCall;
import org.apache.hadoop.util.Lists;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import javax.annotation.CheckForNull;

public class TieredList<T> implements List<T> {
    @SuppressWarnings("rawtypes")
    private static final TieredList GENESIS = new TieredList<>(Collections.emptyList(), Collections.emptyList());
    private final List<T> baseTier;
    private final List<T> tier;
    private final int numTiers;

    private TieredList(List<T> baseTier, List<T> tier) {
        Preconditions.checkArgument(baseTier.isEmpty() || baseTier instanceof TieredList);
        Preconditions.checkArgument(tier.isEmpty() || tier instanceof ImmutableList);
        this.baseTier = Objects.requireNonNull(baseTier);
        this.tier = Objects.requireNonNull(tier);
        this.numTiers = baseTier.isEmpty() ? 0 : ((TieredList<T>) baseTier).numTiers + 1;
    }

    @SuppressWarnings("unchecked")
    public static <T> TieredList<T> genesis() {
        return (TieredList<T>) GENESIS;
    }

    @SuppressWarnings("unchecked")
    public static <T> Builder<T> newGenesisTier() {
        return (Builder<T>) genesis().newTier();
    }

    public static <T> Builder<T> merge(Builder<T> lhs, Builder<T> rhs) {
        return lhs.build().concat(rhs.build()).newTier();
    }

    public static <T> Collector<? super T, TieredList.Builder<T>, TieredList<T>> toList() {
        return Collector.of(
                TieredList::newGenesisTier,
                TieredList.Builder::add,
                TieredList::merge,
                TieredList.Builder::build);
    }

    public Builder<T> newTier() {
        return new Builder<>(this);
    }

    private List<List<T>> untier() {
        if (this == GENESIS) {
            return Lists.newArrayListWithCapacity(numTiers);
        } else {
            List<List<T>> tiers = ((TieredList<T>) baseTier).untier();
            tiers.add(tier);
            return tiers;
        }
    }

    public List<T> untiered() {
        return new ArrayList<>(this);
    }

    public TieredList<T> concat(Collection<T> rhs) {
        Preconditions.checkArgument(rhs != null);
        if (rhs.isEmpty()) {
            return this;
        } else if (rhs instanceof ImmutableList) {
            return new TieredList<>(this, (ImmutableList<T>) rhs);
        } else if (rhs instanceof TieredList) {
            List<List<T>> tiers = ((TieredList<T>) rhs).untier();
            return (TieredList<T>) tiers.stream().reduce(this, (a, b) -> ((TieredList<T>) a).concat(b));
        } else {
            return this.newTier().addAll(rhs).build();
        }
    }

    public TieredList<T> concatOne(T rhs) {
        Preconditions.checkArgument(rhs != null);
        return this.concat(TieredList.<T>newGenesisTier().add(rhs).build());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TieredList<?> that = (TieredList<?>) o;
        return this.untiered().equals(that.untiered());
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseTier, tier, numTiers);
    }

    @Override
    public int size() {
        return baseTier.size() + tier.size();
    }

    @Override
    public boolean isEmpty() {
        return baseTier.isEmpty() && tier.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return tier.contains(o) || baseTier.contains(o);
    }

    @NotNull
    @Override
    public Iterator<T> iterator() {
        return Iterators.concat(baseTier.iterator(), tier.iterator());
    }

    @Override
    public Object[] toArray() {
        //throw new UnsupportedOperationException();
        return this.stream().collect(Collectors.toList()).toArray();
    }

    @Override
    public <T1> T1[] toArray(@NotNull T1[] t1s) {
        return this.stream().collect(Collectors.toList()).toArray(t1s);
    }

    @Override
    @Deprecated
    @CanIgnoreReturnValue
    @DoNotCall("Always throws UnsupportedOperationException")
    public boolean add(T t) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    @CanIgnoreReturnValue
    @DoNotCall("Always throws UnsupportedOperationException")
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    @CanIgnoreReturnValue
    @DoNotCall("Always throws UnsupportedOperationException")
    public boolean containsAll(@NotNull Collection<?> collection) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    @CanIgnoreReturnValue
    @DoNotCall("Always throws UnsupportedOperationException")
    public boolean addAll(@NotNull Collection<? extends T> collection) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    @CanIgnoreReturnValue
    @DoNotCall("Always throws UnsupportedOperationException")
    public boolean addAll(int i, @NotNull Collection<? extends T> collection) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    @CanIgnoreReturnValue
    @DoNotCall("Always throws UnsupportedOperationException")
    public boolean removeAll(@NotNull Collection<?> collection) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    @CanIgnoreReturnValue
    @DoNotCall("Always throws UnsupportedOperationException")
    public boolean retainAll(@NotNull Collection<?> collection) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    @CanIgnoreReturnValue
    @DoNotCall("Always throws UnsupportedOperationException")
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public T get(int i) {
        int sz = size();
        Preconditions.checkArgument(-sz <= i && i < sz);
        int idx = i < 0 ? (sz + i) : i;
        int off = idx - baseTier.size();
        if (off >= 0) {
            return tier.get(off);
        } else {
            return baseTier.get(idx);
        }
    }

    @Override
    @Deprecated
    @CanIgnoreReturnValue
    @CheckForNull
    @DoNotCall("Always throws UnsupportedOperationException")
    public T set(int i, T t) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    @CanIgnoreReturnValue
    @DoNotCall("Always throws UnsupportedOperationException")
    public void add(int i, T t) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    @CanIgnoreReturnValue
    @CheckForNull
    @DoNotCall("Always throws UnsupportedOperationException")
    public T remove(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    @CanIgnoreReturnValue
    @DoNotCall("Always throws UnsupportedOperationException")
    public int indexOf(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    @CanIgnoreReturnValue
    @DoNotCall("Always throws UnsupportedOperationException")
    public int lastIndexOf(Object o) {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    @Deprecated
    @CanIgnoreReturnValue
    @DoNotCall("Always throws UnsupportedOperationException")
    public ListIterator<T> listIterator() {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    @Deprecated
    @CanIgnoreReturnValue
    @DoNotCall("Always throws UnsupportedOperationException")
    public ListIterator<T> listIterator(int i) {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    @Deprecated
    @CanIgnoreReturnValue
    @DoNotCall("Always throws UnsupportedOperationException")
    public List<T> subList(int i, int i1) {
        throw new UnsupportedOperationException();
    }

    private void formatImpl(String name, PrettyPrinter printer) {
        if (this != GENESIS) {
            ((TieredList<T>) baseTier).formatImpl(name, printer);
            printer.add(name).add(".tier#").add(numTiers).newLine();
            printer.indentEnclose(() -> {
                for (int i = 0; i < tier.size(); ++i) {
                    printer.add("[").add(i).add("] = ").add(tier.get(i)).newLine();
                }
            });
        }
    }

    public void format(String name, PrettyPrinter printer) {
        formatImpl(name, printer);
    }

    @Override
    public String toString() {
        PrettyPrinter printer = new PrettyPrinter();
        formatImpl("TieredList", printer);
        return printer.getResult();
    }

    public static class Builder<T> {
        private final TieredList<T> baseTier;
        private final ImmutableList.Builder<T> tierBuilder;

        private boolean isSealed = false;

        private Builder(TieredList<T> baseTier) {
            this.baseTier = baseTier;
            this.tierBuilder = ImmutableList.builder();
        }

        private void checkUnsealed() {
            Preconditions.checkArgument(!isSealed, "TieredList.isSealed");
        }

        public Builder<T> add(T elem) {
            checkUnsealed();
            this.tierBuilder.add(Objects.requireNonNull(elem));
            return this;
        }

        @SafeVarargs
        public final Builder<T> add(T... elems) {
            checkUnsealed();
            this.tierBuilder.add(elems);
            return this;
        }

        public final Builder<T> addAll(Iterable<T> elems) {
            checkUnsealed();
            this.tierBuilder.addAll(elems);
            return this;
        }

        public TieredList<T> build() {
            ImmutableList<T> tier = this.tierBuilder.build();
            if (tier.isEmpty()) {
                return baseTier;
            } else {
                return new TieredList<>(baseTier, tier);
            }
        }

        public void seal() {
            isSealed = true;
        }

        public boolean isSealed() {
            return isSealed;
        }
    }
}
