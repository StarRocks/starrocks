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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/thrift_server.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <thrift/TProcessor.h>
#include <thrift/server/TServer.h>

#include <thread>
#include <unordered_map>

#include "common/status.h"
#include "util/metrics.h"

namespace starrocks {
// Utility class for all Thrift servers. Runs a TNonblockingServer(default) or a
// TThreadPoolServer with, by default, 2 worker threads, that exposes the interface
// described by a user-supplied TProcessor object.
// If TNonblockingServer is used, client must use TFramedTransport.
// If TThreadPoolServer is used, client must use TSocket as transport.
class ThriftServer {
public:
    // An opaque identifier for the current session, which identifies a client connection.
    typedef std::string SessionKey;

    static const int DEFAULT_WORKER_THREADS = 2;

    // There are 3 servers supported by Thrift with different threading models.
    // THREAD_POOL  -- Allocates a fixed number of threads. A thread is used by a
    //                connection until it closes.
    // THREADED     -- Allocates 1 thread per connection, as needed.
    // NON_BLOCKING -- Threads are allocated to a connection only when the server
    //                is working on behalf of the connection.
    enum ServerType { THREAD_POOL = 0, THREADED, NON_BLOCKING };

    // Creates, but does not start, a new server on the specified port
    // that exports the supplied interface.
    //  - name: human-readable name of this server. Should not contain spaces
    //  - processor: Thrift processor to handle RPCs
    //  - port: The port the server will listen for connections on
    //  - metrics: if not nullptr, the server will register metrics on this object
    //  - num_worker_threads: the number of worker threads to use in any thread pool
    //  - server_type: the type of IO strategy this server should employ
    ThriftServer(const std::string& name, std::shared_ptr<apache::thrift::TProcessor> processor, int port,
                 MetricRegistry* metrics = nullptr, int num_worker_threads = DEFAULT_WORKER_THREADS,
                 ServerType server_type = THREADED);

    ~ThriftServer() = default;

    int port() const { return _port; }

    void stop();
    // Blocks until the server stops and exits its main thread.
    void join();

    // Starts the main server thread. Once this call returns, clients
    // may connect to this server and issue RPCs. May not be called more
    // than once.
    Status start();

private:
    // True if the server has been successfully started, for internal use only
    bool _started;

    // True if the server has been stop()
    bool _stopped = false;

    // The port on which the server interface is exposed
    int _port;

    // How many worker threads to use to serve incoming requests
    // (requests are queued if no thread is immediately available)
    int _num_worker_threads;

    // ThreadPool or NonBlocking server
    ServerType _server_type;

    // User-specified identifier that shows up in logs
    const std::string _name;

    // Thread that runs the TNonblockingServer::serve loop
    std::unique_ptr<std::thread> _server_thread;

    // Thrift housekeeping
    std::unique_ptr<apache::thrift::server::TServer> _server;
    std::shared_ptr<apache::thrift::TProcessor> _processor;

    // Protects _session_keys
    std::mutex _session_keys_lock;

    // Map of active session keys to shared_ptr containing that key; when a key is
    // removed it is automatically freed.
    typedef std::unordered_map<SessionKey*, std::shared_ptr<SessionKey> > SessionKeySet;
    SessionKeySet _session_keys;

    // True if metrics are enabled
    bool _metrics_enabled;

    // Number of currently active connections
    std::unique_ptr<IntGauge> _current_connections;

    // Total connections made over the lifetime of this server
    std::unique_ptr<IntCounter> _connections_total;

    // Helper class which monitors starting servers. Needs access to internal members, and
    // is not used outside of this class.
    class ThriftServerEventProcessor;
    friend class ThriftServerEventProcessor;
};

} // namespace starrocks
