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

import com.starrocks.StarRocksFE;
import com.starrocks.common.util.Daemon;
import com.starrocks.server.GlobalStateMgr;
import groovy.lang.Binding;
import org.apache.groovy.groovysh.Groovysh;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.groovy.tools.shell.IO;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;

public class GroovyUDSServer extends Daemon {
    private static final Logger LOG = LogManager.getLogger(GroovyUDSServer.class);
    private static GroovyUDSServer instance = new GroovyUDSServer();

    public static GroovyUDSServer getInstance() {
        return instance;
    }

    private String socketPath =
            (StarRocksFE.STARROCKS_HOME_DIR == null ? "/tmp" : StarRocksFE.STARROCKS_HOME_DIR) + "/groovy_debug.sock";

    GroovyUDSServer() {
        super("GroovyUDSServer");
    }

    public String getSocketPath() {
        return socketPath;
    }

    @Override
    protected void runOneCycle() {
        try {
            // Ensure previous socket is removed
            Files.deleteIfExists(Path.of(socketPath));

            // Create the Unix Domain Socket server
            UnixDomainSocketAddress address = UnixDomainSocketAddress.of(socketPath);
            try (ServerSocketChannel serverChannel = ServerSocketChannel.open(StandardProtocolFamily.UNIX)) {
                serverChannel.bind(address);

                LOG.info("Groovy Shell Server started on {}", socketPath);
                Files.setPosixFilePermissions(Path.of(socketPath), PosixFilePermissions.fromString("rw-------"));

                while (this.isRunning()) {
                    SocketChannel client = serverChannel.accept();
                    new Thread(new ShellSession(client), "GroovySession").start();
                }
            }
        } catch (IOException e) {
            LOG.warn(e.getMessage(), e);
        }
    }

    static class ShellSession implements Runnable {
        private final SocketChannel client;

        ShellSession(SocketChannel client) {
            this.client = client;
        }

        @Override
        public void run() {
            LOG.info("New Groovy Shell session opened");
            try (InputStream in = Channels.newInputStream(client);
                    OutputStream out = Channels.newOutputStream(client)) {
                IO io = new IO(in, out, out);
                Binding binding = new Binding();
                binding.setVariable("LOG", LOG);
                binding.setVariable("out", io.out);
                binding.setVariable("globalState", GlobalStateMgr.getCurrentState());
                binding.setVariable("metastore", GlobalStateMgr.getCurrentState().getLocalMetastore());
                Groovysh shell = new Groovysh(binding, io);
                shell.run("");
                client.close();
            } catch (Exception e) {
                LOG.warn(e.getMessage(), e);
            } finally {
                LOG.info("Groovy Shell session closed");
            }
        }
    }
}
