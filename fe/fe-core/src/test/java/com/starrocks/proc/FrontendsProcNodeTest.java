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

package com.starrocks.proc;

import com.starrocks.common.proc.FrontendsProcNode;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.system.Frontend;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class FrontendsProcNodeTest {

    private InetSocketAddress socketAddr1;

    // Build a real address instead of mocking InetAddress: JDK 19+ makes InetAddress a sealed
    // class, which JMockit 1.x cannot redefine. An address created with an explicit host name
    // returns that name from getHostName() without a reverse lookup, and getHostAddress()
    // returns the literal IP, which is all isJoin() needs.
    private void mockAddress() throws UnknownHostException {
        InetAddress addr1 = InetAddress.getByAddress("sandbox", new byte[] {127, 0, 0, 1});
        socketAddr1 = new InetSocketAddress(addr1, 1000);
    }

    @Test
    public void testIsJoin() throws ClassNotFoundException,
            NoSuchMethodException,
            SecurityException,
            IllegalAccessException,
            IllegalArgumentException,
            InvocationTargetException,
            UnknownHostException {
        mockAddress();
        List<InetSocketAddress> list = new ArrayList<InetSocketAddress>();
        list.add(socketAddr1);

        Class<?> clazz = Class.forName(FrontendsProcNode.class.getName());
        Method isJoin = clazz.getDeclaredMethod("isJoin", List.class, Frontend.class);
        isJoin.setAccessible(true);

        Frontend feCouldNotFoundByPort = new Frontend(FrontendNodeType.LEADER, "test", "127.0.0.1", 2000);
        boolean result = (boolean) isJoin.invoke(FrontendsProcNode.class, list, feCouldNotFoundByPort);
        Assertions.assertFalse(result);

        Frontend feCouldFoundByIP = new Frontend(FrontendNodeType.LEADER, "test", "127.0.0.1", 1000);
        boolean result1 = (boolean) isJoin.invoke(FrontendsProcNode.class, list, feCouldFoundByIP);
        Assertions.assertTrue(result1);

        Frontend feCouldNotFoundByIP = new Frontend(FrontendNodeType.LEADER, "test", "127.0.0.2", 1000);
        boolean result2 = (boolean) isJoin.invoke(FrontendsProcNode.class, list, feCouldNotFoundByIP);
        Assertions.assertTrue(!result2);

        Frontend feCouldFoundByHostName = new Frontend(FrontendNodeType.LEADER, "test", "sandbox", 1000);
        boolean result3 = (boolean) isJoin.invoke(FrontendsProcNode.class, list, feCouldFoundByHostName);
        Assertions.assertTrue(result3);

        Frontend feCouldNotFoundByHostName = new Frontend(FrontendNodeType.LEADER, "test", "sandbox1", 1000);
        boolean result4 = (boolean) isJoin.invoke(FrontendsProcNode.class, list, feCouldNotFoundByHostName);
        Assertions.assertTrue(!result4);

        // Cover the following case:
        // 1. dns name `A.B` can be resolved
        // 2. `A.B` added to FE
        // 3. dns name `A.B` is removed from DNS server, can't be resolved any more
        // 4. run `show frontends`
        list.add(InetSocketAddress.createUnresolved("hostname.can.not.be.resolved", 9010));
        boolean result5 = (boolean) isJoin.invoke(FrontendsProcNode.class, list, feCouldNotFoundByHostName);
        Assertions.assertTrue(!result5);
    }

    @Test
    public void testIPTitle() {
        Assertions.assertTrue(FrontendsProcNode.TITLE_NAMES.get(2).equals("IP"));
    }
}
