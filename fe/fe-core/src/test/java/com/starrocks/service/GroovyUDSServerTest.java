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

package com.starrocks.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class GroovyUDSServerTest {

    private GroovyUDSServer server;

    @BeforeEach
    void setUp() {
        server = GroovyUDSServer.getInstance();
    }

    @Test
    void testSingletonInstance() {
        assertNotNull(server);
        assertSame(server, GroovyUDSServer.getInstance());
    }

    @Test
    void testRunOneCycleSocketCreation() throws IOException, InterruptedException {
        String socketPath = server.getSocketPath();
        Path socketFile = Path.of(socketPath);

        // Start the GroovyUDSServer in a separate thread
        server.start();

        // wait for Files.exists(socketFile) at most 20s
        for (int i = 0; i < 10; i++) {
            Thread.sleep(2000);
            if (Files.exists(socketFile)) {
                break;
            }
        }

        // Connect to the server and send a command
        UnixDomainSocketAddress address = UnixDomainSocketAddress.of(socketPath);
        try (SocketChannel client = SocketChannel.open(StandardProtocolFamily.UNIX)) {
            for (int i = 0; i < 10; i++) {
                try {
                    client.connect(address);
                    break;
                } catch (IOException e) {
                    Thread.sleep(1000);
                    if (i == 9) {
                        throw e;
                    }
                }
            }
            OutputStream out = Channels.newOutputStream(client);
            InputStream in = Channels.newInputStream(client);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            PrintWriter writer = new PrintWriter(out, true);

            // Send a Groovy command and read response
            writer.println("1+1");
            writer.flush();
            assertNotNull(reader.readLine());
            assertNotNull(reader.readLine());
            // Exit the shell
            writer.println(":exit");
            writer.flush();
            try {
                while (true) {
                    String line = reader.readLine();
                    if (line == null) {
                        break;
                    }
                }
            } catch (IOException e) {
                // ignore
            }
        }
        server.setStop();
        Thread.sleep(500);
    }
}
