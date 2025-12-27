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


package com.staros.client;

import com.staros.proto.StatusCode;

public class StarClientHelper {
    private StarClient client = new StarClient();

    public static void printUsageAndExit() {
        System.out.println("usage: StarClientHelper dump star_manager_ip_port");
        System.out.println("usage: StarClientHelper add star_manager_ip_port worker_ip_port");
        System.exit(-1);
    }

    public void dump() {
        try {
            String location = client.dump();
            System.out.println("meta location: " + location + ".");
        } catch (StarClientException e) {
            System.out.println(e);
            System.exit(-1);
        }
    }

    public void add(String workerIpPort) {
        String serviceTemplateName = "StarOSTest";
        String serviceName = "StarOSTest-1";
        try {
            client.registerService(serviceTemplateName);
        } catch (StarClientException e) {
            if (e.getCode() != StatusCode.ALREADY_EXIST) {
                System.out.println(e);
                System.exit(-1);
            }
        }

        try {
            client.bootstrapService(serviceTemplateName, serviceName);
        } catch (StarClientException e) {
            if (e.getCode() != StatusCode.ALREADY_EXIST) {
                System.out.println(e);
                System.exit(-1);
            }
        }

        String serviceId = null;
        try {
            serviceId = client.getServiceInfoByName(serviceName).getServiceId();
        } catch (StarClientException e) {
            System.out.println("fail to get service id.");
            System.out.println(e);
            System.exit(-1);
        }

        System.out.println("add worker " + workerIpPort + " to star manager ...");
        try {
            client.addWorker(serviceId, workerIpPort);
        } catch (StarClientException e) {
            System.out.println(e);
            System.exit(-1);
        }
    }

    public static void main(String[] args) {
        StarClientHelper helper = new StarClientHelper();
        if (args.length < 2) {
            printUsageAndExit();
        }

        System.out.println("connect star manager at " + args[1] + "...");
        helper.client.connectServer(args[1]);

        if (args[0].equals("dump")) {
            helper.dump();
        } else if (args[0].equals("add")) {
            if (args.length != 3) {
                printUsageAndExit();
            }
            helper.add(args[2]);
        } else {
            printUsageAndExit();
        }
    }
}
