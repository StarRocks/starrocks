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

package com.starrocks.rpc;

import com.starrocks.common.util.DnsCache;
import com.starrocks.service.FrontendOptions;
import com.starrocks.thrift.TNetworkAddress;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

public class BrpcProxyTest {

    LakeService getLakeServiceNoException(String hostname, int port) {
        AtomicReference<LakeService> serviceRef = new AtomicReference<>();
        Assertions.assertDoesNotThrow(() -> {
            serviceRef.set(BrpcProxy.getLakeService(hostname, port));
        });
        return serviceRef.get();
    }

    LakeService getLakeServiceNoException(TNetworkAddress address) {
        AtomicReference<LakeService> serviceRef = new AtomicReference<>();
        Assertions.assertDoesNotThrow(() -> {
            serviceRef.set(BrpcProxy.getLakeService(address));
        });
        return serviceRef.get();
    }

    @Test
    public void testConvertToIpAddress(@Mocked FrontendOptions options) {
        // Non-FQDN mode, address always resolves to itself with no change
        new Expectations(options) {
            {
                FrontendOptions.isUseFqdn();
                result = false;
            }
        };
        {
            TNetworkAddress address = new TNetworkAddress("127.0.0.1", 8080);
            Assertions.assertEquals(address, BrpcProxy.convertToIpAddress(address));

            TNetworkAddress address2 = new TNetworkAddress("localhost", 8080);
            Assertions.assertEquals(address2, BrpcProxy.convertToIpAddress(address2));
        }

        // FQDN mode, address resolves to IP if it is a hostname
        new Expectations(options) {
            {
                FrontendOptions.isUseFqdn();
                result = true;
            }
        };
        {
            TNetworkAddress address = new TNetworkAddress("127.0.0.1", 8080);
            Assertions.assertEquals(address, BrpcProxy.convertToIpAddress(address));

            TNetworkAddress address2 = new TNetworkAddress("localhost", 8080);
            Assertions.assertNotEquals(address2, BrpcProxy.convertToIpAddress(address2));
            Assertions.assertEquals(address, BrpcProxy.convertToIpAddress(address2));
        }
    }

    @Test
    public void testGetBrpcService(@Mocked FrontendOptions options) {
        // Non-FQDN mode, service address resolves to itself with no change
        new Expectations(options) {
            {
                FrontendOptions.isUseFqdn();
                result = false;
            }
        };

        TNetworkAddress address1 = new TNetworkAddress("127.0.0.1", 8090);
        TNetworkAddress address2 = new TNetworkAddress("localhost", 8090);
        TNetworkAddress address3 = new TNetworkAddress("127.0.0.1", 8090);
        {
            PBackendService service1 = BrpcProxy.getBackendService(address1);
            PBackendService service2 = BrpcProxy.getBackendService(address2);
            PBackendService service3 = BrpcProxy.getBackendService(address3);
            Assertions.assertNotEquals(service1, service2);
            Assertions.assertEquals(service1, service3);
        }
        {
            LakeService service1 = getLakeServiceNoException(address1);
            LakeService service2 = getLakeServiceNoException(address2);
            LakeService service3 = getLakeServiceNoException(address3);
            Assertions.assertNotEquals(service1, service2);
            Assertions.assertEquals(service1, service3);
        }
        {
            LakeService service1 = getLakeServiceNoException(address1.getHostname(), address1.getPort());
            LakeService service2 = getLakeServiceNoException(address2.getHostname(), address2.getPort());
            LakeService service3 = getLakeServiceNoException(address3.getHostname(), address3.getPort());
            Assertions.assertNotEquals(service1, service2);
            Assertions.assertEquals(service1, service3);

        }

        // FQDN mode, service address resolves to IP if it is a hostname
        new Expectations(options) {
            {
                FrontendOptions.isUseFqdn();
                result = true;
            }
        };
        // All the addresses should resolve to the same service
        {
            PBackendService service1 = BrpcProxy.getBackendService(address1);
            PBackendService service2 = BrpcProxy.getBackendService(address2);
            PBackendService service3 = BrpcProxy.getBackendService(address3);
            Assertions.assertEquals(service1, service2);
            Assertions.assertEquals(service1, service3);
        }
        {
            LakeService service1 = getLakeServiceNoException(address1);
            LakeService service2 = getLakeServiceNoException(address2);
            LakeService service3 = getLakeServiceNoException(address3);
            Assertions.assertEquals(service1, service2);
            Assertions.assertEquals(service1, service3);
        }
        {
            LakeService service1 = getLakeServiceNoException(address1.getHostname(), address1.getPort());
            LakeService service2 = getLakeServiceNoException(address2.getHostname(), address2.getPort());
            LakeService service3 = getLakeServiceNoException(address3.getHostname(), address3.getPort());
            Assertions.assertEquals(service1, service2);
            Assertions.assertEquals(service1, service3);
        }
    }

    @Test
    public void testNonFQDNBrpcResolvesToTheSameService(@Mocked FrontendOptions options, @Mocked DnsCache dnsCache) {
        TNetworkAddress address = new TNetworkAddress("test-123.testdomain", 8090);

        // isUseFqdn is set false to simulate the case that using hostname as the cache key in FQDN mode
        new Expectations(options) {
            {
                FrontendOptions.isUseFqdn();
                result = false;
            }
        };

        new Expectations(dnsCache) {
            {
                // won't invoke this method since isUseFqdn is false
                DnsCache.tryLookup("test-123.testdomain");
                result = "1.2.3.4";
                maxTimes = 0;
            }
        };

        PBackendService serviceA1 = BrpcProxy.getBackendService(address);
        LakeService serviceB1 = getLakeServiceNoException(address);
        LakeService serviceC1 = getLakeServiceNoException(address.getHostname(), address.getPort());

        // Now the domain resolves to a different IP address
        new Expectations(dnsCache) {
            {
                // won't invoke this method since isUseFqdn is false
                DnsCache.tryLookup("test-123.testdomain");
                result = "1.2.3.5";
                maxTimes = 0;
            }
        };

        PBackendService serviceA2 = BrpcProxy.getBackendService(address);
        LakeService serviceB2 = getLakeServiceNoException(address);
        LakeService serviceC2 = getLakeServiceNoException(address.getHostname(), address.getPort());

        // still get the same service instances, which will be a PROBLEM.
        Assertions.assertEquals(serviceA2, serviceA1);
        Assertions.assertEquals(serviceB2, serviceB1);
        Assertions.assertEquals(serviceC2, serviceC1);
    }

    @Test
    public void testFQDNBrpcResolvesToDifferentService(@Mocked FrontendOptions options, @Mocked DnsCache dnsCache) {
        TNetworkAddress address = new TNetworkAddress("test-123.testdomain", 8090);

        new Expectations(options) {
            {
                FrontendOptions.isUseFqdn();
                result = true;
            }
        };

        new Expectations(dnsCache) {
            {
                DnsCache.tryLookup("test-123.testdomain");
                result = "1.2.3.4";
            }
        };

        PBackendService serviceA1 = BrpcProxy.getBackendService(address);
        LakeService serviceB1 = getLakeServiceNoException(address);
        LakeService serviceC1 = getLakeServiceNoException(address.getHostname(), address.getPort());

        // Now the domain resolves to a different IP address
        new Expectations(dnsCache) {
            {
                DnsCache.tryLookup("test-123.testdomain");
                result = "1.2.3.5";
            }
        };

        PBackendService serviceA2 = BrpcProxy.getBackendService(address);
        LakeService serviceB2 = getLakeServiceNoException(address);
        LakeService serviceC2 = getLakeServiceNoException(address.getHostname(), address.getPort());

        // In FQDN mode, resolving to different service instances when the IP address changes is the expected behavior.
        Assertions.assertNotEquals(serviceA2, serviceA1);
        Assertions.assertNotEquals(serviceB2, serviceB1);
        Assertions.assertNotEquals(serviceC2, serviceC1);
    }
}
