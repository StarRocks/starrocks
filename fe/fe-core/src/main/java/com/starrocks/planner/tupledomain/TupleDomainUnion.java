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

package com.starrocks.planner.tupledomain;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class TupleDomainUnion {
    /**
     * Similar to tuple domain, a TupleDomainUnion also describes possible domains of tuples.
     * The difference is that it delays all "union" operations (e.g. or) so that ranges are not combined until required
     * This helps solve the (a=1 and b=1) or (a=10 and b=10) case.
     * TupleDomainUnion is internally represented as a list of TupleDomains.
     * Conceptually, these TupleDomains can be thought of as being OR'ed together to form the representative predicate.
     * <p>
     * This list is normalized in the following ways:
     * 1) The list will not contain TupleDomain.all() as any of its values. If any of the Domain
     * values are TupleDomain.all(), then the whole list will instead be Optional.empty().
     * 2) The list will not contain TupleDomain.none() as any of its values. A domain of none can be ignored by the union.
     * 3) The tuple domains can be overlapping, which can be pruned and merged
     */
    private final Optional<Set<TupleDomain>> tupleDomains;

    public Optional<Set<TupleDomain>> getTupleDomains() {
        return tupleDomains;
    }

    public TupleDomainUnion(Optional<Set<TupleDomain>> tupleDomains) {
        this.tupleDomains = tupleDomains.flatMap(set -> {
            if (set.stream().anyMatch(TupleDomain::isAll)) {
                return Optional.empty();
            }
            return Optional.of(normalizeAndCopy(set));
        });
    }

    public static TupleDomainUnion withTupleDomain(TupleDomain tupleDomain) {
        return new TupleDomainUnion(Optional.of(Collections.singleton(tupleDomain)));
    }

    public static TupleDomainUnion withTupleDomains(Set<TupleDomain> tupleDomains) {
        return new TupleDomainUnion(Optional.of(tupleDomains));
    }

    public static TupleDomainUnion withColumnDomains(Map<String, ColumnDomain> domains) {
        return withTupleDomain(TupleDomain.withColumnDomains(domains));
    }

    public static TupleDomainUnion all() {
        return new TupleDomainUnion(Optional.empty());
    }

    public static TupleDomainUnion none() {
        return withTupleDomains(Collections.emptySet());
    }

    public boolean isAll() {
        return !tupleDomains.isPresent();
    }

    public boolean isNone() {
        return tupleDomains.isPresent() && tupleDomains.get().isEmpty();
    }

    private static Set<TupleDomain> normalizeAndCopy(Set<TupleDomain> tupleDomains)
    {
        return tupleDomains.stream()
                .filter(entry -> !entry.isNone())
                .collect(Collectors.toSet());
    }

    // (a or b) or (c or d) = (a or b or c or d)
    public TupleDomainUnion union(TupleDomainUnion other)
    {
        if (this.isAll() || other.isAll())
            return all();
        if (this.isNone())
            return other;
        if (other.isNone())
            return this;

        Set<TupleDomain> unite = new HashSet<>(getTupleDomains().get());
        unite.addAll(other.getTupleDomains().get());
        return withTupleDomains(unite);
    }

    // (a or b) and (c or d) = (a and c) or (b and c) or (a and d) or (b and d)
    public TupleDomainUnion intersect(TupleDomainUnion other)
    {
        if (this.isNone() || other.isNone())
            return none();
        if (this.isAll())
            return other;
        if (other.isAll())
            return this;

        Set<TupleDomain> intersection = new HashSet<>();
        for (TupleDomain thisDomain : getTupleDomains().get()) {
            for (TupleDomain otherDomain : other.getTupleDomains().get()) {
                TupleDomain intersectDomain = thisDomain.intersect(otherDomain);
                if (intersectDomain.isAll())
                    return TupleDomainUnion.all();
                if (!intersectDomain.isNone())
                    intersection.add(intersectDomain);
            }
        }
        return withTupleDomains(intersection);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("TupleDomainUnion[");
        if (isNone()) {
            builder.append("None");
        } else if (isAll()) {
            builder.append("All");
        }
        builder.append(
                tupleDomains.get().stream().map(TupleDomain::toString).collect(Collectors.joining(" or ")));
        builder.append("]");
        return builder.toString();
    }
}
