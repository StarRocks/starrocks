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

package com.starrocks.extension;

import com.google.common.collect.Maps;
import com.starrocks.persist.gson.DefaultGsonBuilderFactory;
import com.starrocks.persist.gson.IGsonBuilderFactory;
import com.starrocks.qe.scheduler.slot.BaseSlotManager;
import com.starrocks.qe.scheduler.slot.ResourceUsageMonitor;
import com.starrocks.qe.scheduler.slot.SlotManager;
import com.starrocks.server.WarehouseManager;

import java.util.Map;

public class DefaultExtensionContext implements ExtensionContext {
    private final Map<Class<?>, Object> capabilityMap = Maps.newHashMap();

    public DefaultExtensionContext() {
        registerDefault();
    }

    @Override
    public <T> void register(Class<T> clazz, T instance) {
        if (clazz == null || instance == null) {
            throw new IllegalArgumentException("Capability class or instance cannot be null");
        }
        capabilityMap.put(clazz, instance);
    }

    @Override
    public <T> T get(Class<T> clazz) {
        Object instance = capabilityMap.get(clazz);
        if (instance == null) {
            throw new IllegalStateException("Capability not registered: " + clazz.getName());
        }
        return (T) instance;
    }

    public void registerDefault() {
        register(WarehouseManager.class, new WarehouseManager());
        register(ResourceUsageMonitor.class, new ResourceUsageMonitor());
        register(BaseSlotManager.class, new SlotManager(get(ResourceUsageMonitor.class)));
        register(IGsonBuilderFactory.class, new DefaultGsonBuilderFactory());
    }
}
