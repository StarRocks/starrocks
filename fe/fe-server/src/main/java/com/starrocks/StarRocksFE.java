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

package com.starrocks;

import com.google.common.base.Strings;
import com.starrocks.common.CommandLineOptions;
import com.starrocks.journal.bdbje.BDBToolOptions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class StarRocksFE {
    public static void main(String[] args) {
        CommandLineOptions options = parseArgs(args);
        StarRocksFEServer.start(options);
    }

    /*
     * -v --version
     *      Print the version of StarRocks Frontend
     * -h --helper
     *      Specify the helper node when joining a bdb je replication group
     * -b --bdb
     *      Run bdbje debug tools
     *
     *      -l --listdb
     *          List all database names in bdbje
     *      -d --db
     *          Specify a database in bdbje
     *
     *          -s --stat
     *              Print statistic of a database, including count, first key, last key
     *          -f --from
     *              Specify the start scan key
     *          -t --to
     *              Specify the end scan key
     *          -m --metaversion
     *              Specify the meta version to decode log value, separated by ',', first
     *              is community meta version, second is StarRocks meta version
     * -rs --cluster_snapshot
     *      Specify fe start to restore from a cluster snapshot
     * -ht --host_type
     *      Specify fe start use ip or fqdn
     * -fp --failpoint
     *      Enable fail point
     */
    protected static CommandLineOptions parseArgs(String[] args) {
        CommandLineParser commandLineParser = new DefaultParser();
        Options options = getOptions();

        CommandLine cmd = null;
        try {
            cmd = commandLineParser.parse(options, args);
        } catch (final ParseException e) {
            System.err.println("Failed to parse command line. exit now");
            System.exit(-1);
        }

        CommandLineOptions commandLineOptions = new CommandLineOptions();
        // -v --version
        if (cmd.hasOption('v') || cmd.hasOption("version")) {
            commandLineOptions.setVersion(true);
        }
        // -b --bdb
        if (cmd.hasOption('b') || cmd.hasOption("bdb")) {
            if (cmd.hasOption('l') || cmd.hasOption("listdb")) {
                // list bdb je databases
                BDBToolOptions bdbOpts = new BDBToolOptions(true, "", false, "", "", 0, 0);
                commandLineOptions.setBdbToolOpts(bdbOpts);
            } else if (cmd.hasOption('d') || cmd.hasOption("db")) {
                // specify a database
                String dbName = cmd.getOptionValue("db");
                if (Strings.isNullOrEmpty(dbName)) {
                    System.err.println("BDBJE database name is missing");
                    System.exit(-1);
                }

                if (cmd.hasOption('s') || cmd.hasOption("stat")) {
                    BDBToolOptions bdbOpts = new BDBToolOptions(false, dbName, true, "", "", 0, 0);
                    commandLineOptions.setBdbToolOpts(bdbOpts);
                } else {
                    String fromKey = "";
                    String endKey = "";
                    int metaVersion = 0;
                    int starrocksMetaVersion = 0;
                    if (cmd.hasOption('f') || cmd.hasOption("from")) {
                        fromKey = cmd.getOptionValue("from");
                        if (Strings.isNullOrEmpty(fromKey)) {
                            System.err.println("from key is missing");
                            System.exit(-1);
                        }
                    }
                    if (cmd.hasOption('t') || cmd.hasOption("to")) {
                        endKey = cmd.getOptionValue("to");
                        if (Strings.isNullOrEmpty(endKey)) {
                            System.err.println("end key is missing");
                            System.exit(-1);
                        }
                    }
                    if (cmd.hasOption('m') || cmd.hasOption("metaversion")) {
                        try {
                            String version = cmd.getOptionValue("metaversion");
                            String[] vs = version.split(",");
                            if (vs.length != 2) {
                                System.err.println("invalid meta version format");
                                System.exit(-1);
                            }
                            metaVersion = Integer.parseInt(vs[0]);
                            starrocksMetaVersion = Integer.parseInt(vs[1]);
                        } catch (NumberFormatException e) {
                            System.err.println("Invalid meta version format");
                            System.exit(-1);
                        }
                    }

                    BDBToolOptions bdbOpts =
                            new BDBToolOptions(false, dbName, false, fromKey, endKey, metaVersion,
                                    starrocksMetaVersion);
                    commandLineOptions.setBdbToolOpts(bdbOpts);
                }
            } else {
                System.err.println("Invalid options when running bdb je tools");
                System.exit(-1);
            }
        }
        // -h --helper
        if (cmd.hasOption('h') || cmd.hasOption("helper")) {
            String helperNode = cmd.getOptionValue("helper");
            if (Strings.isNullOrEmpty(helperNode)) {
                System.err.println("Missing helper node value");
                System.exit(-1);
            }
            commandLineOptions.setHelpers(helperNode);
        }
        // -ht --host_type
        if (cmd.hasOption("ht") || cmd.hasOption("host_type")) {
            String hostType = cmd.getOptionValue("host_type");
            if (Strings.isNullOrEmpty(hostType)) {
                System.err.println("Missing host type value");
                System.exit(-1);
            }
            commandLineOptions.setHostType(hostType);
        }
        // -rs --cluster_snapshot
        if (cmd.hasOption("rs") || cmd.hasOption("cluster_snapshot")) {
            commandLineOptions.setStartFromSnapshot(true);
        }
        // -fp --failpoint
        if (cmd.hasOption("fp") || cmd.hasOption("failpoint")) {
            commandLineOptions.setEnableFailPoint(true);
        }

        return commandLineOptions;
    }

    private static Options getOptions() {
        Options options = new Options();
        options.addOption("ht", "host_type", true, "Specify fe start use ip or fqdn");
        options.addOption("rs", "cluster_snapshot", false, "Specify fe start to restore from a cluster snapshot");
        options.addOption("v", "version", false, "Print the version of StarRocks Frontend");
        options.addOption("h", "helper", true, "Specify the helper node when joining a bdb je replication group");
        options.addOption("b", "bdb", false, "Run bdbje debug tools");
        options.addOption("l", "listdb", false, "Print the list of databases in bdbje");
        options.addOption("d", "db", true, "Specify a database in bdbje");
        options.addOption("s", "stat", false, "Print statistic of a database, including count, first key, last key");
        options.addOption("f", "from", true, "Specify the start scan key");
        options.addOption("t", "to", true, "Specify the end scan key");
        options.addOption("m", "metaversion", true,
                "Specify the meta version to decode log value, separated by ',', first is community meta" +
                        " version, second is StarRocks meta version");
        options.addOption("fp", "failpoint", false, "enable fail point");
        return options;
    }
}