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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class TupleDomain {
    /**
     * Describes the domain of tuples by giving the ColumnDomain of a subset of columns
     * Conceptually, these Domains can be thought of as being AND'ed together to form the representative predicate.
     * <p>
     * This map is normalized in the following ways:
     * 1) The map will not contain ColumnDomain.none() as any of its values. If any of the Domain
     * values are none(), then the whole map will instead be Optional.empty(). This enforces the fact that
     * any single ColumnDomain.none() value effectively turns this TupleDomain into "none" as well.
     * 2) The map will not contain ColumnDomain.all() as any of its values. Our convention here is that
     * any unmentioned column is equivalent to having all().
     */
    private final Optional<Map<String, ColumnDomain>> domains;

    public Optional<Map<String, ColumnDomain>> getDomains()
    {
        return domains;
    }

    private TupleDomain(Optional<Map<String, ColumnDomain>> domains) {
        this.domains = domains.flatMap(domain -> {
            if (domain.values().stream().anyMatch(ColumnDomain::isNone)) {
                return Optional.empty();
            }
            return Optional.of(normalizeAndCopy(domain));
        });
    }

    public static TupleDomain withColumnDomains(Map<String, ColumnDomain> domains)
    {
        return new TupleDomain(Optional.of(domains));
    }

    public static TupleDomain none()
    {
        return new TupleDomain(Optional.empty());
    }

    public static TupleDomain all()
    {
        return withColumnDomains(Collections.emptyMap());
    }

    public boolean isNone() {
        return !domains.isPresent();
    }

    public boolean isAll() {
        return domains.isPresent() && domains.get().isEmpty();
    }

    private static Map<String, ColumnDomain> normalizeAndCopy(Map<String, ColumnDomain> domains)
    {
        return domains.entrySet().stream()
                .filter(entry -> !entry.getValue().isAll())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final TupleDomain other = (TupleDomain) obj;
        return Objects.equals(this.domains, other.domains);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(domains);
    }

    public TupleDomain intersect(TupleDomain other)
    {
        if (this == other)
            return this;
        if (this.isNone() || other.isNone())
            return none();
        if (this.isAll())
            return other;
        if (other.isAll())
            return this;

        Map<String, ColumnDomain> intersected = new HashMap<>(this.getDomains().get());
        for (Map.Entry<String, ColumnDomain> entry : other.getDomains().get().entrySet()) {
            if (!intersected.containsKey(entry.getKey())) {
                intersected.put(entry.getKey(), entry.getValue());
            } else {
                ColumnDomain intersection = intersected.get(entry.getKey()).intersect(entry.getValue());
                if (intersection.isNone()) {
                    return TupleDomain.none();
                }
                intersected.put(entry.getKey(), intersection);
            }
        }
        return withColumnDomains(intersected);
    }

    public static TupleDomain columnWiseUnion(TupleDomain first, TupleDomain second, TupleDomain... rest)
    {
        List<TupleDomain> tupleDomains = new ArrayList<>(rest.length + 2);
        tupleDomains.add(first);
        tupleDomains.add(second);
        tupleDomains.addAll(Arrays.asList(rest));

        return columnWiseUnion(tupleDomains);
    }

    public static TupleDomain columnWiseUnion(List<TupleDomain> tupleDomains)
    {
        if (tupleDomains.isEmpty()) {
            throw new IllegalArgumentException("tupleDomains must have at least one element");
        }

        if (tupleDomains.size() == 1) {
            return tupleDomains.get(0);
        }

        // gather all common columns
        Set<String> commonColumns = new HashSet<>();

        // first, find a non-none domain
        boolean found = false;
        Iterator<TupleDomain> domains = tupleDomains.iterator();
        while (domains.hasNext()) {
            TupleDomain domain = domains.next();
            if (domain.isAll()) {
                // all or any = all
                return TupleDomain.all();
            }
            if (!domain.isNone()) {
                found = true;
                commonColumns.addAll(domain.getDomains().get().keySet());
                break;
            }
        }

        if (!found) {
            return TupleDomain.none();
        }

        // then, get the common columns. a column has domain if it exists in every one in the union
        while (domains.hasNext()) {
            TupleDomain domain = domains.next();
            if (!domain.isNone()) {
                // if a column is missing in some domain, its range is infinity
                commonColumns.retainAll(domain.getDomains().get().keySet());
            }
        }

        // group domains by column (only for common columns)
        Map<String, List<ColumnDomain>> domainsByColumn = new HashMap<>(tupleDomains.size());

        for (TupleDomain domain : tupleDomains) {
            if (!domain.isNone()) {
                for (Map.Entry<String, ColumnDomain> entry : domain.getDomains().get().entrySet()) {
                    if (commonColumns.contains(entry.getKey())) {
                        List<ColumnDomain> columnDomain = domainsByColumn.get(entry.getKey());
                        if (columnDomain == null) {
                            columnDomain = new ArrayList<>();
                            domainsByColumn.put(entry.getKey(), columnDomain);
                        }
                        columnDomain.add(entry.getValue());
                    }
                }
            }
        }

        // finally, do the column-wise union
        Map<String, ColumnDomain> result = new HashMap<>(domainsByColumn.size());
        for (Map.Entry<String, List<ColumnDomain>> entry : domainsByColumn.entrySet()) {
            result.put(entry.getKey(), entry.getValue().stream().reduce(ColumnDomain::union).get());
        }
        return withColumnDomains(result);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("TupleDomain{");
        if (isNone()) {
            builder.append("None");
        } else if (isAll()) {
            builder.append("All");
        }
        builder.append(
                domains.get().entrySet().stream().map(entry -> entry.getKey() + "=" + entry.getValue().toString()).collect(
                        Collectors.joining(" and ")));
        builder.append("}");
        return builder.toString();
    }

}
