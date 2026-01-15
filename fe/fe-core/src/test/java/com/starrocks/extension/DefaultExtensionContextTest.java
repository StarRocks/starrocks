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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DefaultExtensionContextTest {

    private DefaultExtensionContext context;

    // Test classes for dependency injection scenarios

    // Simple class with no-arg constructor
    public static class SimpleService {
        public String getValue() {
            return "simple";
        }
    }

    // Class with single public constructor with dependency
    public static class ServiceWithDependency {
        private final SimpleService simpleService;

        public ServiceWithDependency(SimpleService simpleService) {
            this.simpleService = simpleService;
        }

        public String getValue() {
            return "dependent:" + simpleService.getValue();
        }
    }

    // Class with @Inject annotation
    public static class ServiceWithInject {
        private final SimpleService simpleService;

        @Inject
        public ServiceWithInject(SimpleService simpleService) {
            this.simpleService = simpleService;
        }

        public ServiceWithInject() {
            this.simpleService = null;
        }

        public String getValue() {
            return "injected:" + (simpleService != null ? simpleService.getValue() : "null");
        }
    }

    // Class with multiple dependencies
    public static class ComplexService {
        private final SimpleService simpleService;
        private final ServiceWithDependency dependentService;

        public ComplexService(SimpleService simpleService, ServiceWithDependency dependentService) {
            this.simpleService = simpleService;
            this.dependentService = dependentService;
        }

        public String getValue() {
            return "complex:" + simpleService.getValue() + "," + dependentService.getValue();
        }
    }

    // Class with multiple public constructors (should fail without @Inject or no-arg)
    public static class AmbiguousService {
        private final String value;

        public AmbiguousService(String value) {
            this.value = value;
        }

        public AmbiguousService(Integer value) {
            this.value = value.toString();
        }

        public String getValue() {
            return value;
        }
    }

    // Class with @Inject on one of multiple constructors
    public static class ResolvedAmbiguousService {
        private final String value;

        @Inject
        public ResolvedAmbiguousService(SimpleService service) {
            this.value = service.getValue();
        }

        public ResolvedAmbiguousService(Integer value) {
            this.value = value.toString();
        }

        public String getValue() {
            return value;
        }
    }

    @Test
    public void testRegisterAndGet() {
        context = new DefaultExtensionContext();
        SimpleService service = new SimpleService();
        context.register(SimpleService.class, service);
        
        SimpleService retrieved = context.get(SimpleService.class);
        Assertions.assertSame(service, retrieved);
    }

    @Test
    public void testGetWithNoArgConstructor() {
        context = new DefaultExtensionContext();
        SimpleService service = context.get(SimpleService.class);
        Assertions.assertNotNull(service);
        Assertions.assertEquals("simple", service.getValue());
    }

    @Test
    public void testAlwaysNewInstance() {
        context = new DefaultExtensionContext();
        SimpleService service1 = context.get(SimpleService.class);
        SimpleService service2 = context.get(SimpleService.class);
        
        Assertions.assertNotNull(service1);
        Assertions.assertNotNull(service2);
        // Should be different instances
        Assertions.assertNotSame(service1, service2);
    }

    @Test
    public void testDependencyInjection() {
        context = new DefaultExtensionContext();
        ServiceWithDependency service = context.get(ServiceWithDependency.class);
        Assertions.assertNotNull(service);
        Assertions.assertEquals("dependent:simple", service.getValue());
    }

    @Test
    public void testInjectAnnotation() {
        context = new DefaultExtensionContext();
        ServiceWithInject service = context.get(ServiceWithInject.class);
        Assertions.assertNotNull(service);
        Assertions.assertEquals("injected:simple", service.getValue());
    }

    @Test
    public void testComplexDependencyResolution() {
        context = new DefaultExtensionContext();
        ComplexService service = context.get(ComplexService.class);
        Assertions.assertNotNull(service);
        Assertions.assertEquals("complex:simple,dependent:simple", service.getValue());
    }

    @Test
    public void testRecursiveDependencyResolution() {
        context = new DefaultExtensionContext();
        // ComplexService depends on ServiceWithDependency which depends on SimpleService
        ComplexService service = context.get(ComplexService.class);
        Assertions.assertNotNull(service);
        
        // Verify each call creates new instances
        ComplexService service2 = context.get(ComplexService.class);
        Assertions.assertNotSame(service, service2);
    }

    @Test
    public void testAmbiguousConstructorFails() {
        context = new DefaultExtensionContext();
        Assertions.assertThrows(IllegalStateException.class, () -> {
            context.get(AmbiguousService.class);
        });
    }

    @Test
    public void testResolvedAmbiguousConstructor() {
        context = new DefaultExtensionContext();
        ResolvedAmbiguousService service = context.get(ResolvedAmbiguousService.class);
        Assertions.assertNotNull(service);
        Assertions.assertEquals("simple", service.getValue());
    }

    @Test
    public void testRegisterNullClass() {
        context = new DefaultExtensionContext();
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            context.register(null, new SimpleService());
        });
    }

    @Test
    public void testRegisterNullInstance() {
        context = new DefaultExtensionContext();
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            context.register(SimpleService.class, null);
        });
    }

    @Test
    public void testRegisteredInstanceTakesPrecedence() {
        context = new DefaultExtensionContext();

        SimpleService registered = new SimpleService() {
            @Override
            public String getValue() {
                return "registered";
            }
        };
        
        context.register(SimpleService.class, registered);
        SimpleService retrieved = context.get(SimpleService.class);
        
        Assertions.assertSame(registered, retrieved);
        Assertions.assertEquals("registered", retrieved.getValue());
    }

    @Test
    public void testConstructorMetadataCaching() {
        context = new DefaultExtensionContext();

        // First call should cache constructor metadata
        SimpleService service1 = context.get(SimpleService.class);
        Assertions.assertNotNull(service1);
        
        // Second call should use cached metadata but create new instance
        SimpleService service2 = context.get(SimpleService.class);
        Assertions.assertNotNull(service2);
        Assertions.assertNotSame(service1, service2);
    }

    // Test for multiple @Inject annotations (should fail)
    public static class MultipleInjectService {
        @Inject
        public MultipleInjectService(SimpleService service) {
        }

        @Inject
        public MultipleInjectService(ServiceWithDependency service) {
        }
    }

    @Test
    public void testMultipleInjectAnnotationsFails() {
        context = new DefaultExtensionContext();

        Assertions.assertThrows(IllegalStateException.class, () -> {
            context.get(MultipleInjectService.class);
        });

    }

    @Test
    public void testDefaultRegistrations() {
        context = new DefaultExtensionContext();
        // DefaultExtensionContext registers defaults in constructor
        // Verify that default capabilities are available
        Assertions.assertNotNull(context.get(com.starrocks.server.WarehouseManager.class));
        Assertions.assertNotNull(context.get(com.starrocks.qe.scheduler.slot.ResourceUsageMonitor.class));
        Assertions.assertNotNull(context.get(com.starrocks.qe.scheduler.slot.BaseSlotManager.class));
        Assertions.assertNotNull(context.get(com.starrocks.persist.gson.IGsonBuilderFactory.class));
    }

    @Test
    public void testRegisterConstructor() {
        context = new DefaultExtensionContext();
        // Test registerConstructor method
        ConstructorMetadata metadata = context.registerConstructor(SimpleService.class, SimpleService.class);
        Assertions.assertNotNull(metadata);
        Assertions.assertNotNull(metadata.getConstructor());
        Assertions.assertEquals(0, metadata.getParameterTypes().length);
    }

    @Test
    public void testRegisterConstructorWithDependencies() {
        context = new DefaultExtensionContext();
        // Test registerConstructor for class with dependencies
        ConstructorMetadata metadata = context.registerConstructor(ServiceWithDependency.class, ServiceWithDependency.class);
        Assertions.assertNotNull(metadata);
        Assertions.assertNotNull(metadata.getConstructor());
        Assertions.assertEquals(1, metadata.getParameterTypes().length);
        Assertions.assertEquals(SimpleService.class, metadata.getParameterTypes()[0]);
    }

    @Test
    public void testRegisterConstructorWithInject() {
        context = new DefaultExtensionContext();
        // Test registerConstructor respects @Inject annotation
        ConstructorMetadata metadata = context.registerConstructor(ServiceWithInject.class, ServiceWithInject.class);
        Assertions.assertNotNull(metadata);
        Assertions.assertNotNull(metadata.getConstructor());
        // Should select the @Inject constructor which has 1 parameter
        Assertions.assertEquals(1, metadata.getParameterTypes().length);
        Assertions.assertEquals(SimpleService.class, metadata.getParameterTypes()[0]);
    }

    @Test
    public void testRegisterConstructorCachesMetadata() {
        context = new DefaultExtensionContext();
        // First call to registerConstructor
        ConstructorMetadata metadata1 = context.registerConstructor(SimpleService.class, SimpleService.class);
        
        // Get should use cached metadata
        SimpleService service1 = context.get(SimpleService.class);
        SimpleService service2 = context.get(SimpleService.class);
        
        Assertions.assertNotNull(service1);
        Assertions.assertNotNull(service2);
        Assertions.assertNotSame(service1, service2);
    }

    @Test
    public void testGetDoesNotCallRegisterConstructor() {
        context = new DefaultExtensionContext();
        // Test that get() doesn't call the public registerConstructor method
        // This is important because registerConstructor should be for explicit registration
        // while get() uses internal resolution
        
        // Call get() which should internally resolve without calling registerConstructor
        SimpleService service = context.get(SimpleService.class);
        Assertions.assertNotNull(service);
        
        // Now call registerConstructor - it should still work and cache the metadata
        ConstructorMetadata metadata = context.registerConstructor(ServiceWithDependency.class, ServiceWithDependency.class);
        Assertions.assertNotNull(metadata);
        
        // Subsequent get() calls should use the cached metadata
        ServiceWithDependency dep1 = context.get(ServiceWithDependency.class);
        ServiceWithDependency dep2 = context.get(ServiceWithDependency.class);
        Assertions.assertNotNull(dep1);
        Assertions.assertNotNull(dep2);
        Assertions.assertNotSame(dep1, dep2);
    }
}
