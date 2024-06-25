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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.DoNotCall;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.CheckForNull;

public class TieredMap<K, V> implements Map<K, V> {
    @SuppressWarnings("rawtypes")
    private static final TieredMap<?, ?> GENESIS = new TieredMap<>(Collections.emptyMap(), Collections.emptyMap());
    private final Map<K, V> baseTier;
    private final Map<K, V> tier;
    private final int numTiers;

    private TieredMap(Map<K, V> baseTier, Map<K, V> tier) {
        this.baseTier = baseTier;
        this.tier = tier;
        this.numTiers = baseTier.isEmpty() ? 0 : ((TieredMap<K, V>) baseTier).numTiers + 1;
    }

    @SuppressWarnings("unchecked")
    public static <K, V> TieredMap.Builder<K, V> newGenesisTier() {
        return (TieredMap.Builder<K, V>) genesis().newTier();
    }

    @SuppressWarnings("unchecked")
    public static <K, V> TieredMap<K, V> genesis() {
        return (TieredMap<K, V>) GENESIS;
    }

    private static <K, V> Builder<K, V> merge(Builder<K, V> a, Builder<K, V> b) {
        return a.build().merge(b.build()).newTier();
    }

    public static <K, V> Collector<? super Map.Entry<? extends K, ? extends V>, Builder<K, V>, TieredMap<K, V>> toMap() {
        return Collector.of(
                TieredMap::newGenesisTier,
                Builder::put,
                TieredMap::merge,
                Builder::build);
    }

    public static <T, K, V> Collector<T, Builder<K, V>, TieredMap<K, V>> toMap(
            Function<T, ? extends K> keyMapper,
            Function<T, ? extends V> valueMapper) {
        return Collector.of(
                TieredMap::newGenesisTier,
                (builder, item) -> builder.put(keyMapper.apply(item), valueMapper.apply(item)),
                TieredMap::merge,
                Builder::build);
    }

    public Builder<K, V> newTier() {
        return new Builder<>(this);
    }

    private List<Map<K, V>> untier() {
        if (this == GENESIS) {
            return Lists.newArrayListWithCapacity(this.numTiers);
        } else {
            Preconditions.checkArgument(baseTier instanceof TieredMap);
            List<Map<K, V>> tiers = ((TieredMap<K, V>) baseTier).untier();
            tiers.add(tier);
            return tiers;
        }
    }

    public TieredMap<K, V> merge(Map<K, V> rhs) {
        Preconditions.checkArgument(rhs != null);
        if (rhs.isEmpty() || rhs == this) {
            return this;
        } else if (rhs instanceof ImmutableMap) {
            return new TieredMap<>(this, rhs);
        } else if (rhs instanceof TieredMap) {
            List<Map<K, V>> tiers = ((TieredMap<K, V>) rhs).untier();
            return (TieredMap<K, V>) tiers.stream().reduce(this, (a, b) -> ((TieredMap<K, V>) a).merge(b));
        } else {
            return this.newTier().putAll(rhs).build();
        }
    }

    @Override
    public int size() {
        return tier.size() + baseTier.size();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object o) {
        return tier.containsKey(o) || baseTier.containsKey(o);
    }

    @Override
    public boolean containsValue(Object o) {
        return tier.containsValue(o) || baseTier.containsValue(o);
    }

    @Override
    public V get(Object o) {
        return Optional.ofNullable(tier.get(o)).orElseGet(() -> baseTier.get(o));
    }

    @Override
    @Deprecated
    @CheckForNull
    @CanIgnoreReturnValue
    @DoNotCall("Always throws UnsupportedOperationException")
    public final V put(K k, V v) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    @CheckForNull
    @CanIgnoreReturnValue
    @DoNotCall("Always throws UnsupportedOperationException")
    public final V remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    @CanIgnoreReturnValue
    @DoNotCall("Always throws UnsupportedOperationException")
    public void putAll(Map<? extends K, ? extends V> map) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    @CanIgnoreReturnValue
    @DoNotCall("Always throws UnsupportedOperationException")
    public final void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<K> keySet() {
        return Sets.union(tier.keySet(), baseTier.keySet());
    }

    @Override
    public Collection<V> values() {
        return Stream.of(tier.values(), baseTier.values())
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return Sets.union(tier.entrySet(), baseTier.entrySet());
    }

    private void formatImpl(String name, PrettyPrinter printer) {
        if (this != GENESIS) {
            ((TieredMap<K, V>) baseTier).formatImpl(name, printer);
            printer.indentEnclose(() -> {
                printer.add(name).add(".tier#").add(numTiers).newLine();
                printer.indentEnclose(() -> {
                    tier.forEach((key, value) -> printer.add("{").add(key).add("} = ").add(value).newLine());
                });
            });
        }
    }

    public void format(String name, PrettyPrinter printer) {
        formatImpl(name, printer);
    }

    public Map<K, V> untiered() {
        return this.entrySet().stream().collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TieredMap<?, ?> tieredMap = (TieredMap<?, ?>) o;
        return this.untiered().equals(tieredMap.untiered());
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseTier, tier, numTiers);
    }

    @Override
    public String toString() {
        PrettyPrinter printer = new PrettyPrinter();
        formatImpl("TieredMap", printer);
        return printer.getResult();
    }

    public static class Builder<K, V> {
        private final TieredMap<K, V> baseTier;
        private final ImmutableMap.Builder<K, V> tierBuilder;

        private Builder(TieredMap<K, V> baseTier) {
            this.baseTier = Objects.requireNonNull(baseTier);
            this.tierBuilder = ImmutableMap.builder();
        }

        @CanIgnoreReturnValue
        public Builder<K, V> put(K key, V value) {
            this.tierBuilder.put(Objects.requireNonNull(key), Objects.requireNonNull(value));
            return this;
        }

        @CanIgnoreReturnValue
        public Builder<K, V> putAll(Map<K, V> map) {
            this.tierBuilder.putAll(Objects.requireNonNull(map));
            return this;
        }

        @CanIgnoreReturnValue
        public Builder<K, V> put(Map.Entry<? extends K, ? extends V> entry) {
            this.tierBuilder.put(Objects.requireNonNull(entry));
            return this;
        }

        @CanIgnoreReturnValue
        public Builder<K, V> putAll(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) {
            this.tierBuilder.putAll(Objects.requireNonNull(entries));
            return this;
        }

        public TieredMap<K, V> build() {
            ImmutableMap<K, V> tier = tierBuilder.build();
            if (tier.isEmpty()) {
                return baseTier;
            } else {
                return new TieredMap<>(baseTier, tier);
            }
        }
    }
}
