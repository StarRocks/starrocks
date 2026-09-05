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

package com.starrocks.authentication;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedSet;
import com.starrocks.sql.analyzer.SemanticException;

import java.util.Map;

public class GroupProviderFactory {

    private static final ImmutableSortedSet<String> SUPPORT_GROUP_PROVIDER =
            ImmutableSortedSet.orderedBy(String.CASE_INSENSITIVE_ORDER)
                    .add(UnixGroupProvider.TYPE)
                    .add(FileGroupProvider.TYPE)
                    .add(LDAPGroupProvider.TYPE)
                    .build();

    public static void checkGroupProviderIsSupported(String groupProviderType) {
        if (!SUPPORT_GROUP_PROVIDER.contains(groupProviderType)) {
            throw new SemanticException("unsupported group provider type '" + groupProviderType + "'");
        }
    }
    public static GroupProvider createGroupProvider(String name, Map<String, String> propertyMap) {
        String type = propertyMap.get(GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY);
        checkGroupProviderIsSupported(type);

        GroupProvider groupProvider = null;
        if (type.equalsIgnoreCase(FileGroupProvider.TYPE)) {
            groupProvider = new FileGroupProvider(name, propertyMap);
        } else if (type.equalsIgnoreCase(UnixGroupProvider.TYPE)) {
            groupProvider = new UnixGroupProvider(name, propertyMap);
        } else if (type.equalsIgnoreCase(LDAPGroupProvider.TYPE)) {
            groupProvider = new LDAPGroupProvider(name, propertyMap);
        }

        Preconditions.checkNotNull(groupProvider);
        return groupProvider;
    }
}
