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

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.Map;

public class DefaultExtensionContext implements ExtensionContext {
    private final Map<Class<?>, Object> capabilityMap = Maps.newHashMap();
    private final Map<Class<?>, ConstructorMetadata> constructorMetadataMap = Maps.newHashMap();

    /**
     * Stores metadata about a constructor for dependency injection.
     */
    private static class ConstructorMetadata {
        private final Constructor<?> constructor;
        private final Class<?>[] parameterTypes;

        public ConstructorMetadata(Constructor<?> constructor) {
            this.constructor = constructor;
            this.parameterTypes = constructor.getParameterTypes();
            // Ensure constructor is accessible
            if (!constructor.isAccessible()) {
                constructor.setAccessible(true);
            }
        }

        public Constructor<?> getConstructor() {
            return constructor;
        }

        public Class<?>[] getParameterTypes() {
            return parameterTypes;
        }
    }

    public DefaultExtensionContext() {
        registerDefault();
    }

    /**
     * Register a pre-existing instance for a given class.
     * This provides legacy support for explicit instance registration.
     */
    @Override
    public <T> void register(Class<T> clazz, T instance) {
        if (clazz == null || instance == null) {
            throw new IllegalArgumentException("Capability class or instance cannot be null");
        }
        capabilityMap.put(clazz, instance);
    }

    /**
     * Get an instance of the specified class.
     * - First checks capabilityMap for registered instances (legacy support)
     * - Then attempts to create a new instance using dependency injection
     * - Constructor metadata is cached for performance
     */
    @Override
    public <T> T get(Class<T> clazz) {
        // First check if instance is explicitly registered (legacy support)
        Object instance = capabilityMap.get(clazz);
        if (instance != null) {
            return (T) instance;
        }

        // Use dependency injection to create a new instance
        return createInstance(clazz);
    }

    /**
     * Create a new instance of the specified class using dependency injection.
     * Always returns a new instance - instances are never cached.
     */
    private <T> T createInstance(Class<T> clazz) {
        try {
            // Get or resolve constructor metadata
            ConstructorMetadata metadata = constructorMetadataMap.get(clazz);
            if (metadata == null) {
                metadata = resolveConstructor(clazz);
                constructorMetadataMap.put(clazz, metadata);
            }

            // Recursively resolve parameters
            Object[] parameters = new Object[metadata.getParameterTypes().length];
            for (int i = 0; i < metadata.getParameterTypes().length; i++) {
                parameters[i] = get(metadata.getParameterTypes()[i]);
            }

            // Create and return new instance
            return clazz.cast(metadata.getConstructor().newInstance(parameters));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create instance of " + clazz.getName(), e);
        }
    }

    /**
     * Resolve which constructor to use for dependency injection.
     * Rules:
     * 1. Prefer constructors annotated with @Inject
     * 2. Otherwise, use the single public constructor
     * 3. Otherwise, use the no-arg constructor
     * 4. Throw an exception for ambiguity
     */
    public ConstructorMetadata resolveConstructor(Class<?> clazz) {
        Constructor<?>[] constructors = clazz.getDeclaredConstructors();

        // Rule 1: Look for @Inject annotated constructor
        Constructor<?> injectConstructor = null;
        for (Constructor<?> constructor : constructors) {
            if (constructor.isAnnotationPresent(Inject.class)) {
                if (injectConstructor != null) {
                    throw new IllegalStateException(
                            "Multiple constructors annotated with @Inject in " + clazz.getName());
                }
                injectConstructor = constructor;
            }
        }
        if (injectConstructor != null) {
            return new ConstructorMetadata(injectConstructor);
        }

        // Rule 2: Look for single public constructor
        Constructor<?> publicConstructor = null;
        for (Constructor<?> constructor : constructors) {
            if (Modifier.isPublic(constructor.getModifiers())) {
                if (publicConstructor != null) {
                    // Multiple public constructors, fall through to Rule 3
                    publicConstructor = null;
                    break;
                }
                publicConstructor = constructor;
            }
        }
        if (publicConstructor != null) {
            return new ConstructorMetadata(publicConstructor);
        }

        // Rule 3: Look for no-arg constructor
        try {
            Constructor<?> noArgConstructor = clazz.getDeclaredConstructor();
            return new ConstructorMetadata(noArgConstructor);
        } catch (NoSuchMethodException e) {
            // No no-arg constructor found
        }

        // Rule 4: Throw exception for ambiguity
        throw new IllegalStateException(
                "Cannot resolve constructor for " + clazz.getName() + 
                ": multiple public constructors exist and no @Inject annotation or no-arg constructor found");
    }

    /**
     * Register default capabilities.
     * Creates initial instances and registers them in the capability map.
     * After registration, these instances will be returned by get() instead of creating new ones.
     */
    public void registerDefault() {
        // Create and register instances using dependency injection where applicable
        ResourceUsageMonitor resourceUsageMonitor = new ResourceUsageMonitor();
        register(ResourceUsageMonitor.class, resourceUsageMonitor);
        
        register(WarehouseManager.class, new WarehouseManager());
        register(BaseSlotManager.class, new SlotManager(resourceUsageMonitor));
        register(IGsonBuilderFactory.class, new DefaultGsonBuilderFactory());
    }
}
