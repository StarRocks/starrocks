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

#pragma once

#include "exec/data_sink.h"

namespace starrocks {

/**
 * @brief The AsyncDataSink class is a data sink that can handle multiple OLAP table sinks.
 * 
 * This class inherits from the DataSink class and provides methods for preparing, opening, adding chunks, closing
 */
class AsyncDataSink : public DataSink {
public:
    /**
     * @brief Tries to open the AsyncDataSink asynchronously.
     * 
     * This method is called to try opening the sink asynchronously.
     * call order: try_open() -> [is_open_done()] -> open_wait()
     * if is_open_done() return true, open_wait() will not block
     * otherwise open_wait() will block
     * 
     * @param state The runtime state.
     * @return The status of the try open operation.
     */
    virtual Status try_open(RuntimeState* state) = 0;

    /**
     * @brief Checks if the AsyncDataSink is open.
     * 
     * This method is called to check if the sink is open.
     * 
     * @return True if the sink is open, false otherwise.
     */
    virtual bool is_open_done() = 0;

    /**
     * @brief Waits for the AsyncDataSink to open.
     * 
     * This method is called to wait for the sink to finish opening.
     * 
     * @return The status of the open wait operation.
     */
    virtual Status open_wait() = 0;

    /**
     * @brief Checks if the AsyncDataSink is full.
     * 
     * This method is called to check if the sink is full and cannot accept more data.
     * 
     * @return True if the sink is full, false otherwise.
     */
    virtual bool is_full() = 0;

    // async add chunk interface
    virtual Status send_chunk_nonblocking(RuntimeState* state, Chunk* chunk) = 0;

    /**
     * @brief Tries to close the AsyncDataSink asynchronously.
     * 
     * This method is called to try closing the sink asynchronously.
     * call order: try_close() -> [is_close_done()] -> close_wait()
     * if is_close_done() return true, close_wait() will not block
     * otherwise close_wait() will block
     * 
     * @param state The runtime state.
     * @return The status of the try close operation.
     */
    virtual Status try_close(RuntimeState* state) = 0;

    /**
     * @brief Waits for the AsyncDataSink to close.
     * 
     * This method is called to wait for the sink to finish closing.
     * 
     * @param state The runtime state.
     * @param close_status The status of the close operation.
     * @return The status of the close wait operation.
     */
    virtual Status close_wait(RuntimeState* state, Status close_status) = 0;

    /**
     * @brief Checks if the AsyncDataSink is closed.
     * 
     * This method is called to check if the sink is closed.
     * 
     * @return True if the sink is closed, false otherwise.
     */
    virtual bool is_close_done() = 0;

    virtual void set_profile(RuntimeProfile* profile) = 0;
};

} // namespace starrocks