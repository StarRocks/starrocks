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

package com.starrocks.planner;

import com.google.common.collect.Lists;
import com.starrocks.common.util.DnsCache;
import com.starrocks.thrift.TNetworkAddress;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class DictionaryCacheSinkTest {

    // The node list in this sink is shipped to a BE, which turns every entry into a stub via
    // HttpBrpcStubCache::get_http_stub(). That cache is keyed by the resolved EndPoint, so it cannot
    // suppress the lookup -- a hostname here means one uncached getaddrinfo per node per refresh on the
    // BE, which has no DNS cache. The sink must therefore carry resolved addresses. It is done here
    // rather than in DictionaryMgr#fillBackendsOrComputeNodes on purpose: that helper also feeds
    // SHOW DICTIONARY, which must keep printing the hostname the user configured.
    @Test
    public void testSinkCarriesResolvedAddresses() {
        new MockUp<DnsCache>() {
            @Mock
            public String tryLookup(String hostname) {
                if ("be-0.starrocks-be.svc.cluster.local".equals(hostname)) {
                    return "10.0.0.11";
                } else if ("cn-0.starrocks-cn.svc.cluster.local".equals(hostname)) {
                    return "10.0.0.12";
                }
                return hostname;
            }
        };

        List<TNetworkAddress> nodes = Lists.newArrayList(
                new TNetworkAddress("be-0.starrocks-be.svc.cluster.local", 8060),
                new TNetworkAddress("cn-0.starrocks-cn.svc.cluster.local", 8060));

        DictionaryCacheSink sink = new DictionaryCacheSink(nodes, null, 1L);

        Assertions.assertEquals(
                Lists.newArrayList(new TNetworkAddress("10.0.0.11", 8060), new TNetworkAddress("10.0.0.12", 8060)),
                sink.getNodes());
        // The caller's list must not be mutated: DictionaryMgr reuses it for the BEGIN/COMMIT/CLEAR RPCs
        // it issues itself, and those are expected to keep going through BrpcProxy by hostname.
        Assertions.assertEquals("be-0.starrocks-be.svc.cluster.local", nodes.get(0).getHostname());
    }

    // An address that is already an IP literal must survive untouched, and an unresolvable hostname must
    // fall back to itself rather than becoming null or empty -- so the change can only ever degrade to
    // the previous behavior.
    @Test
    public void testResolutionIsIdempotentAndFallsBack() {
        List<TNetworkAddress> nodes = Lists.newArrayList(
                new TNetworkAddress("10.0.0.11", 8060),
                new TNetworkAddress("no-such-host.invalid", 8060));

        DictionaryCacheSink sink = new DictionaryCacheSink(nodes, null, 1L);

        Assertions.assertEquals(new TNetworkAddress("10.0.0.11", 8060), sink.getNodes().get(0));
        Assertions.assertEquals(new TNetworkAddress("no-such-host.invalid", 8060), sink.getNodes().get(1));
    }
}
