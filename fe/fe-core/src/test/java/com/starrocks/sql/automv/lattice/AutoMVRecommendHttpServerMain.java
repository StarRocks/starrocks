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

package com.starrocks.sql.automv.lattice;

import com.google.common.base.Preconditions;
import com.starrocks.http.HttpServer;
import com.starrocks.http.IllegalArgException;

public class AutoMVRecommendHttpServerMain {
    public static void main(String[] args) throws IllegalArgException {
        if (args.length != 1) {
            System.err.println("Number of arguments must be 1");
            System.exit(1);
        }

        int port = -1;
        try {
            port = Integer.parseInt(args[0]);
            Preconditions.checkState(0 < port && port <= 65535, "port must range 0..65535");
        } catch (Throwable ex) {
            System.err.println("Fail to parse port");
            ex.printStackTrace();
            System.exit(0);
        }

        HttpServer httpServer = new HttpServer(port);
        AutoMVRecommendAction.registerAction(httpServer.getController());
        httpServer.start();
    }
}