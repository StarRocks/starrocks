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

package com.staros.provisioner;

import com.staros.util.Config;
import com.staros.util.LogConfig;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

/**
 * Convenient entry-point to run a standalone StarProvisionServer for testing
 *
 * java -cp <starmgr.jar> com.staros.provisioner.StarProvisionServerMain [starmgr.conf] [persistent_datadir]
 */
public class StarProvisionServerMain {
    private static final Logger LOG = LogManager.getLogger(StarProvisionServerMain.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length >= 1) {
            String confFile = args[0];
            new Config().init(confFile);
        }
        LogConfig.initLogging();

        if (args.length >= 2) {
            LOG.info("Use BUILTIN_PROVISION_SERVER_DATA_DIR = {} ...", args[1]);
            Config.BUILTIN_PROVISION_SERVER_DATA_DIR = args[1];
        }

        int port = Config.STARMGR_RPC_PORT;
        ServerBuilder<?> builder = ServerBuilder.forPort(port);
        StarProvisionServer provisionServer = new StarProvisionServer();
        StarProvisionServer.getServices(provisionServer).forEach(builder::addService);
        Server server = builder.build();

        String msg = "Starting StarProvisionServer on port " + port + " ...";
        System.err.println(msg);
        LOG.info(msg);
        server.start();
        server.awaitTermination();
    }
}
