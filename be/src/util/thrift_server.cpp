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
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/thrift_server.cpp

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

#include "util/thrift_server.h"

#include <thrift/concurrency/Thread.h>
#include <thrift/concurrency/ThreadFactory.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TNonblockingServer.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TNonblockingServerSocket.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TSocket.h>

#include <condition_variable>
#include <memory>
#include <sstream>
#include <thread>
#include <utility>

#include "util/thread.h"

namespace starrocks {

// Helper class that starts a server in a separate thread, and handles
// the inter-thread communication to monitor whether it started
// correctly.
class ThriftServer::ThriftServerEventProcessor : public apache::thrift::server::TServerEventHandler {
public:
    ThriftServerEventProcessor(ThriftServer* thrift_server) : _thrift_server(thrift_server) {}

    // friendly to code style
    ~ThriftServerEventProcessor() override = default;

    // Called by TNonBlockingServer when server has acquired its resources and is ready to
    // serve, and signals to StartAndWaitForServer that start-up is finished.
    // From TServerEventHandler.
    void preServe() override;

    // Called when a client connects; we create per-client state and call any
    // SessionHandlerIf handler.
    void* createContext(std::shared_ptr<apache::thrift::protocol::TProtocol> input,
                        std::shared_ptr<apache::thrift::protocol::TProtocol> output) override;

    // Called when a client starts an RPC; we set the thread-local session key.
    void processContext(void* context, std::shared_ptr<apache::thrift::transport::TTransport> output) override;

    // Called when a client disconnects; we call any SessionHandlerIf handler.
    void deleteContext(void* serverContext, std::shared_ptr<apache::thrift::protocol::TProtocol> input,
                       std::shared_ptr<apache::thrift::protocol::TProtocol> output) override;

    // Waits for a timeout of TIMEOUT_MS for a server to signal that it has started
    // correctly.
    Status start_and_wait_for_server();

private:
    // Lock used to ensure that there are no missed notifications between starting the
    // supervision thread and calling _signal_cond.timed_wait. Also used to ensure
    // thread-safe access to members of _thrift_server
    std::mutex _signal_lock;

    // Condition variable that is notified by the supervision thread once either
    // a) all is well or b) an error occurred.
    std::condition_variable _signal_cond;

    // The ThriftServer under management. This class is a friend of ThriftServer, and
    // reaches in to change member variables at will.
    ThriftServer* _thrift_server;

    // Guards against spurious condition variable wakeups
    bool _signal_fired{false};

    // The time, in milliseconds, to wait for a server to come up
    static constexpr int TIMEOUT_MS = 2500;

    // Called in a separate thread; wraps TNonBlockingServer::serve in an exception handler
    void supervise();
};

Status ThriftServer::ThriftServerEventProcessor::start_and_wait_for_server() {
    // Locking here protects against missed notifications if Supervise executes quickly
    std::unique_lock<std::mutex> lock(_signal_lock);
    _thrift_server->_started = false;

    _thrift_server->_server_thread =
            std::make_unique<std::thread>(&ThriftServer::ThriftServerEventProcessor::supervise, this);
    Thread::set_thread_name(_thrift_server->_server_thread.get()->native_handle(), "thrift_server");

    // Loop protects against spurious wakeup. Locks provide necessary fences to ensure
    // visibility.
    while (!_signal_fired) {
        // Yields lock and allows supervision thread to continue and signal
        if (_signal_cond.wait_for(lock, std::chrono::milliseconds(TIMEOUT_MS)) == std::cv_status::timeout) {
            std::stringstream ss;
            ss << "ThriftServer '" << _thrift_server->_name << "' (on port: " << _thrift_server->_port
               << ") did not start within " << TIMEOUT_MS << "ms";
            LOG(ERROR) << ss.str();
            return Status::InternalError(ss.str());
        }
    }

    // _started == true only if preServe was called. May be false if there was an exception
    // after preServe that was caught by Supervise, causing it to reset the error condition.
    if (_thrift_server->_started == false) {
        std::stringstream ss;
        ss << "ThriftServer '" << _thrift_server->_name << "' (on port: " << _thrift_server->_port
           << ") did not start correctly ";
        LOG(ERROR) << ss.str();
        return Status::InternalError(ss.str());
    }

    return Status::OK();
}

void ThriftServer::ThriftServerEventProcessor::supervise() {
    DCHECK(_thrift_server->_server.get() != nullptr);

    // serve() will call preServe() internal to set _started = true before serve loop start
    try {
        _thrift_server->_server->serve();
    } catch (apache::thrift::TException& e) {
        LOG(ERROR) << "ThriftServer '" << _thrift_server->_name << "' (on port: " << _thrift_server->_port
                   << ") exited due to TException: " << e.what();
    }
    // There are three scenario of serve loop
    // 1. Accept timeout and client disconnect - ThriftServer continue processing.
    // 2. Server was interrupted.  This only happens when stop() called. - ThriftServer serve return
    // 3. All other transport exceptions are only logged. - ThriftServer serve return.
    // In scenario 3 we prefer to retry before stop() called, it will reset the transport
    while (!_thrift_server->_stopped && _thrift_server->_started) {
        try {
            _thrift_server->_server->serve();
            if (!_thrift_server->_stopped) {
                LOG(ERROR) << "ThriftServer '" << _thrift_server->_name << "' (on port: " << _thrift_server->_port
                           << ") exited unexpected. ";
            }
        } catch (apache::thrift::TException& e) {
            LOG(ERROR) << "ThriftServer '" << _thrift_server->_name << "' (on port: " << _thrift_server->_port
                       << ") exited due to TException: " << e.what();
        }
        SleepFor(MonoDelta::FromSeconds(3));
    }

    {
        // _signal_lock ensures mutual exclusion of access to _thrift_server
        std::lock_guard<std::mutex> lock(_signal_lock);
        _thrift_server->_started = false;

        // There may not be anyone waiting on this signal (if the
        // exception occurs after startup). That's not a problem, this is
        // just to avoid waiting for the timeout in case of a bind
        // failure, for example.
        _signal_fired = true;
    }

    _signal_cond.notify_all();
}

void ThriftServer::ThriftServerEventProcessor::preServe() {
    // Acquire the signal lock to ensure that StartAndWaitForServer is
    // waiting on _signal_cond when we notify.
    std::lock_guard<std::mutex> lock(_signal_lock);
    _signal_fired = true;

    // This is the (only) success path - if this is not reached within TIMEOUT_MS,
    // StartAndWaitForServer will indicate failure.
    _thrift_server->_started = true;

    // Should only be one thread waiting on _signal_cond, but wake all just in case.
    _signal_cond.notify_all();
}

// This thread-local variable contains the current session key for whichever
// thrift server is currently serving a request on the current thread.
__thread ThriftServer::SessionKey* _session_key;

ThriftServer::SessionKey* ThriftServer::get_thread_session_key() {
    return _session_key;
}

void* ThriftServer::ThriftServerEventProcessor::createContext(
        std::shared_ptr<apache::thrift::protocol::TProtocol> input,
        std::shared_ptr<apache::thrift::protocol::TProtocol> output) {
    std::stringstream ss;

    apache::thrift::transport::TSocket* socket = nullptr;
    apache::thrift::transport::TTransport* transport = input->getTransport().get();
    {
        switch (_thrift_server->_server_type) {
        case NON_BLOCKING:
            socket = static_cast<apache::thrift::transport::TSocket*>(
                    static_cast<apache::thrift::transport::TFramedTransport*>(transport)
                            ->getUnderlyingTransport()
                            .get());
            break;

        case THREAD_POOL:
        case THREADED:
            socket = static_cast<apache::thrift::transport::TSocket*>(
                    static_cast<apache::thrift::transport::TBufferedTransport*>(transport)
                            ->getUnderlyingTransport()
                            .get());
            break;

        default:
            DCHECK(false) << "Unexpected thrift server type";
        }
    }

    ss << socket->getPeerAddress() << ":" << socket->getPeerPort();

    {
        std::lock_guard<std::mutex> _l(_thrift_server->_session_keys_lock);

        std::shared_ptr<SessionKey> key_ptr(new std::string(ss.str()));

        _session_key = key_ptr.get();
        _thrift_server->_session_keys[key_ptr.get()] = key_ptr;
    }

    if (_thrift_server->_session_handler != nullptr) {
        _thrift_server->_session_handler->session_start(*_session_key);
    }

    if (_thrift_server->_metrics_enabled) {
        _thrift_server->_connections_total->increment(1L);
        _thrift_server->_current_connections->increment(1L);
    }

    // Store the _session_key in the per-client context to avoid recomputing
    // it. If only this were accessible from RPC method calls, we wouldn't have to
    // mess around with thread locals.
    return (void*)_session_key;
}

void ThriftServer::ThriftServerEventProcessor::processContext(
        void* context, std::shared_ptr<apache::thrift::transport::TTransport> transport) {
    _session_key = reinterpret_cast<SessionKey*>(context);
}

void ThriftServer::ThriftServerEventProcessor::deleteContext(
        void* serverContext, std::shared_ptr<apache::thrift::protocol::TProtocol> input,
        std::shared_ptr<apache::thrift::protocol::TProtocol> output) {
    _session_key = (SessionKey*)serverContext;

    if (_thrift_server->_session_handler != nullptr) {
        _thrift_server->_session_handler->session_end(*_session_key);
    }

    {
        std::lock_guard<std::mutex> _l(_thrift_server->_session_keys_lock);
        _thrift_server->_session_keys.erase(_session_key);
    }

    if (_thrift_server->_metrics_enabled) {
        _thrift_server->_current_connections->increment(-1L);
    }
}

ThriftServer::ThriftServer(const std::string& name, std::shared_ptr<apache::thrift::TProcessor> processor, int port,
                           MetricRegistry* metrics, int num_worker_threads, ServerType server_type)
        : _started(false),
          _port(port),
          _num_worker_threads(num_worker_threads),
          _server_type(server_type),
          _name(name),
          _server_thread(nullptr),
          _server(nullptr),
          _processor(std::move(processor)),
          _session_handler(nullptr) {
    if (metrics != nullptr) {
        _metrics_enabled = true;
        _current_connections = std::make_unique<IntGauge>(MetricUnit::CONNECTIONS);
        metrics->register_metric("thrift_current_connections", MetricLabels().add("name", name),
                                 _current_connections.get());

        _connections_total = std::make_unique<IntCounter>(MetricUnit::CONNECTIONS);
        metrics->register_metric("thrift_connections_total", MetricLabels().add("name", name),
                                 _connections_total.get());
    } else {
        _metrics_enabled = false;
    }
}

Status ThriftServer::start() {
    DCHECK(!_started);
    std::shared_ptr<apache::thrift::protocol::TProtocolFactory> protocol_factory(
            new apache::thrift::protocol::TBinaryProtocolFactory());
    std::shared_ptr<apache::thrift::concurrency::ThreadManager> thread_mgr;
    std::shared_ptr<apache::thrift::concurrency::ThreadFactory> thread_factory(
            new apache::thrift::concurrency::ThreadFactory());
    std::shared_ptr<apache::thrift::transport::TServerTransport> fe_server_transport;
    std::shared_ptr<apache::thrift::transport::TTransportFactory> transport_factory;

    if (_server_type != THREADED) {
        thread_mgr = apache::thrift::concurrency::ThreadManager::newSimpleThreadManager(_num_worker_threads);
        thread_mgr->threadFactory(thread_factory);
        thread_mgr->start();
    }

    // Note - if you change the transport types here, you must check that the
    // logic in createContext is still accurate.
    apache::thrift::transport::TServerSocket* server_socket = nullptr;

    switch (_server_type) {
    case NON_BLOCKING: {
        if (transport_factory == nullptr) {
            transport_factory.reset(new apache::thrift::transport::TTransportFactory());
        }

        std::shared_ptr<apache::thrift::transport::TNonblockingServerSocket> port(
                new apache::thrift::transport::TNonblockingServerSocket(_port));
        _server = std::make_unique<apache::thrift::server::TNonblockingServer>(
                _processor, transport_factory, transport_factory, protocol_factory, protocol_factory, port, thread_mgr);
        break;
    }

    case THREAD_POOL:
        fe_server_transport.reset(new apache::thrift::transport::TServerSocket(_port));

        if (transport_factory == nullptr) {
            transport_factory.reset(new apache::thrift::transport::TBufferedTransportFactory());
        }

        _server = std::make_unique<apache::thrift::server::TThreadPoolServer>(
                _processor, fe_server_transport, transport_factory, protocol_factory, thread_mgr);
        break;

    case THREADED:
        server_socket = new apache::thrift::transport::TServerSocket(_port);
        //      server_socket->setAcceptTimeout(500);
        fe_server_transport.reset(server_socket);

        if (transport_factory == nullptr) {
            transport_factory.reset(new apache::thrift::transport::TBufferedTransportFactory());
        }

        _server = std::make_unique<apache::thrift::server::TThreadedServer>(
                _processor, fe_server_transport, transport_factory, protocol_factory, thread_factory);
        break;

    default:
        std::stringstream error_msg;
        error_msg << "Unsupported server type: " << _server_type;
        LOG(ERROR) << error_msg.str();
        return Status::InternalError(error_msg.str());
    }

    std::shared_ptr<ThriftServer::ThriftServerEventProcessor> event_processor(
            new ThriftServer::ThriftServerEventProcessor(this));
    _server->setServerEventHandler(event_processor);

    RETURN_IF_ERROR(event_processor->start_and_wait_for_server());

    LOG(INFO) << _name << " has started listening port on " << _port;

    DCHECK(_started);
    return Status::OK();
}

void ThriftServer::stop() {
    _stopped = true;
    _server->stop();
}

void ThriftServer::join() {
    DCHECK(_server_thread != nullptr);
    DCHECK(_started);
    _server_thread->join();
}

void ThriftServer::stop_for_testing() {
    DCHECK(_server_thread != nullptr);
    DCHECK(_server);
    DCHECK_EQ(_server_type, THREADED);
    _server->stop();

    if (_started) {
        join();
    }
}
} // namespace starrocks
