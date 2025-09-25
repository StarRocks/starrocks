# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# -*- coding: utf-8 -*-

import argparse
import logging
import os

import grpc
import starclient.grpc_gen.manager_pb2 as starmgrpb
import starclient.grpc_gen.manager_pb2_grpc as starmgr_grpc
import starclient.grpc_gen.test_rsmgr_service_pb2 as rsmgrpb
import starclient.grpc_gen.worker_pb2 as workerpb
import starclient.grpc_gen.shard_pb2 as shardpb
import starclient.grpc_gen.service_pb2 as servicepb
import starclient.grpc_gen.test_rsmgr_service_pb2_grpc as rsmgr_grpc


def add_node(args):
    server = args.server
    worker = "%s:%d" % (args.host, args.port)
    logging.info("Send AddNode(%s) RPC to server:%s", worker, server)
    with grpc.insecure_channel(server) as channel:
        stub = rsmgr_grpc.TestResourceManageServiceStub(channel)
        result = stub.addNode(rsmgrpb.AddResourceNodeRequest(host=worker))
        handle_rpc_result(result, "AddResourceNode")


def handle_rpc_result(result, rpc_name):
    if not result:
        logging.error("Fail to send %s GRPC request", rpc_name)
    else:
        code = 0
        if hasattr(result.status, "code"):
            code = result.status.code
        elif hasattr(result.status, "status_code"):
            code = result.status.status_code

        if code != 0:
            logging.info("%s GRPC fail. error: %s", rpc_name, result)
        else:
            logging.info("%s GRPC success.", rpc_name)
            logging.info("Result: %s", result)


def list_workergroup(args):
    kargs = dict()
    kargs["service_id"] = args.serviceid
    kargs["include_workers_info"] = args.include_workerinfo
    if args.workergroupid is not None:
        kargs["group_ids"] = [args.workergroupid]
    logging.info("List workergroup:%d detailed info ....", args.workergroupid or -1)
    with grpc.insecure_channel(args.server) as channel:
        stub = starmgr_grpc.StarManagerStub(channel)
        result = stub.ListWorkerGroup(workerpb.ListWorkerGroupRequest(**kargs))
        handle_rpc_result(result, "ListWorkerGroup")


def list_shard(args):
    kargs = dict()
    kargs["service_id"] = args.serviceid
    kargs["worker_group_id"] = args.workergroupid
    kargs["shard_id"] = [args.shardid]
    logging.info("List Shard:%d in workerGroup:%d detailed info ....", args.shardid, args.workergroupid)
    with grpc.insecure_channel(args.server) as channel:
        stub = starmgr_grpc.StarManagerStub(channel)
        result = stub.GetShard(shardpb.GetShardRequest(**kargs))
        handle_rpc_result(result, "GetShard")


def list_service(args):
    kargs = dict()
    if args.serviceid is not None:
        kargs['service_id'] = args.serviceid
    if args.servicename is not None:
        kargs['service_name'] = args.servicename
    if not kargs:
        logging.error("Must provide one of --serviceid or --servicename!")
        return
    logging.info("List Service info with args:%s ....", kargs)
    with grpc.insecure_channel(args.server) as channel:
        stub = starmgr_grpc.StarManagerStub(channel)
        result = stub.GetService(servicepb.GetServiceRequest(**kargs))
        handle_rpc_result(result, "GetService")


def dump_meta(args):
    server = args.server
    logging.info("Request dump starmgr meta ....")
    with grpc.insecure_channel(server) as channel:
        stub = starmgr_grpc.StarManagerStub(channel)
        result = stub.Dump(starmgrpb.DumpRequest())
        handle_rpc_result(result, "Dump")


def build_addcmd_subparser(parent_parser):
    add_parser = parent_parser.add_parser('add').add_subparsers()
    # add node ...
    addnode_parser = add_parser.add_parser('node')
    addnode_parser.add_argument('--host', type=str, metavar='HOST', required=True,
                                help='host to be added (ip or fqdn)')
    addnode_parser.add_argument('-p', '--port', type=int, metavar='PORT', required=True,
                                help='host port to be added')
    addnode_parser.set_defaults(func=add_node)


def build_lscmd_subparser(parent_parser):
    ls_parser = parent_parser.add_parser('ls').add_subparsers()
    # ls workergroup ...
    lsworkergroup_parser = ls_parser.add_parser('workergroup')
    lsworkergroup_parser.add_argument('-g', '--workergroupid', type=int, metavar='WORKERGROUPID', required=False,
                                      help="Optional, worker group id to be listed, if absent, list all worker groups")
    lsworkergroup_parser.add_argument('-i', '--serviceid', type=str, metavar='SERVICEID', required=True,
                                      help="service id which the group belongs to")
    lsworkergroup_parser.add_argument('-w', '--include-workerinfo', required=False, action='store_true',
                                      help="Optional, if provided, workers info in the group will be also listed.")
    lsworkergroup_parser.set_defaults(func=list_workergroup)

    # ls shard ...
    lsshard_parser = ls_parser.add_parser('shard', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    lsshard_parser.add_argument('-d', '--shardid', type=int, metavar='SHARDID', required=True,
                                help="shard id to be listed")
    lsshard_parser.add_argument('-g', '--workergroupid', type=int, metavar='WORKERGROUPID', required=True,
                                help="worker group id to be listed", default=0)
    lsshard_parser.add_argument('-i', '--serviceid', type=str, metavar='SERVICEID', required=True,
                                help="service id which the group belongs to")
    lsshard_parser.set_defaults(func=list_shard)

    # ls service ...
    lsservice_parser = ls_parser.add_parser('service', formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                            description='list service by service id or by service name')
    lsservice_parser.add_argument('-i', '--serviceid', type=str, metavar='SERVICEID', required=False,
                                  help="service id to be listed")
    lsservice_parser.add_argument('-n', '--servicename', type=str, metavar='SERVICENAME', required=False,
                                  help="service name to be listed.", default="starrocks")
    lsservice_parser.set_defaults(func=list_service)


def main():
    logging.basicConfig(level=logging.INFO)
    default_server = '127.0.0.1:6090'
    if "SERVER_ADDRESS" in os.environ:
        default_server = os.environ['SERVER_ADDRESS']
    parser = argparse.ArgumentParser(description='starclient', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-s', '--server', metavar='SERVER', type=str, dest='server',
                        help='starManager server address.',
                        default=default_server)

    subparser = parser.add_subparsers(title='sub commands', description='additional help')
    build_addcmd_subparser(subparser)
    build_lscmd_subparser(subparser)

    parser_dumpmeta = subparser.add_parser('dumpmeta', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser_dumpmeta.set_defaults(func=dump_meta)

    args = parser.parse_args()
    try:
        args.func(args)
    except AttributeError:
        parser.print_help()
