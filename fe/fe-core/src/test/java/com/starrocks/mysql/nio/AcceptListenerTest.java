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

package com.starrocks.mysql.nio;

import com.starrocks.qe.ConnectScheduler;
import org.junit.jupiter.api.Test;
import org.xnio.StreamConnection;
import org.xnio.channels.AcceptingChannel;

import java.net.InetSocketAddress;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AcceptListenerTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testConnectionIdExhaustionClosesConnectionBeforeScheduling() throws Exception {
        ConnectScheduler scheduler = mock(ConnectScheduler.class);
        AcceptingChannel<StreamConnection> channel = mock(AcceptingChannel.class);
        StreamConnection connection = mock(StreamConnection.class);
        when(channel.accept()).thenReturn(connection);
        when(connection.getPeerAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 9030));
        when(scheduler.getNextConnectionId()).thenThrow(
                new ConnectScheduler.ConnectionIdExhaustedException("no connection ID"));

        new AcceptListener(scheduler).handleEvent(channel);

        verify(connection).close();
        verify(channel, never()).getWorker();
        verify(scheduler, never()).registerConnection(any());
    }
}
