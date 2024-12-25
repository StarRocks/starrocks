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

#include <arrow/array.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/result.h>
#include <arrow/type.h>

namespace starrocks::lake::format {

/**
 * StarRocksFormatWriter is a tablet file writer, which is used to bypass BE Server to write tablet files that stored
 * on the file system (e.g. S3, HDFS) in share-data mode. The written data files can be read directly by StarRocks or
 * by the StarRocksFormatReader provided by this SDK.
 *
 * StarRocksFormatWriter can be regarded as a wrapper for StarRocks TabletWriter, and all file operation methods will
 * eventually be mapped to the corresponding methods in TabletWriter.
 *
 * You can refer to the following example to use:
 * @code
 * // Create and open Writer
 * auto&& result = StarRocksFormatWriter::create(...);
 * StarRocksFormatWriter* format_writer = std::move(result).ValueUnsafe();
 * writer->open();
 *
 * // Begin transaction
 * int32_t cnt = 0;
 * for(...) {
 *   writer->write(arrow_array);
 *   if(++cnt > threshold) {
 *       writer->flush();
 *   }
 * }
 *
 * writer->finish();
 * writer->close();
 *
 * // Commit transaction
 */
class StarRocksFormatWriter {
public:
    static arrow::Result<StarRocksFormatWriter*> create(int64_t tablet_id, std::string tablet_root_path, int64_t txn_id,
                                                        const ArrowSchema* output_schema,
                                                        const std::unordered_map<std::string, std::string> options);

    /**
     * Create a starrocks format writer instance, which bound to a specific tablet.
     *
     * @param tablet_id The target tablet id.
     * @param tablet_root_path The target tablet root path.
     * @param txn_id The transaction ID that the current writing process belongs.
     * @param output_schema The table schema that the target tablet belongs.
     * @param options key-value parameters, e.g. S3 connection authentication configuration.
     */
    static arrow::Result<StarRocksFormatWriter*> create(int64_t tablet_id, std::string tablet_root_path, int64_t txn_id,
                                                        const std::shared_ptr<arrow::Schema> output_schema,
                                                        const std::unordered_map<std::string, std::string> options);

    virtual ~StarRocksFormatWriter() = default;

    /**
     * Open the format writer, the StarRocks TabletWriter will be initialized and opened during this time.
     */
    virtual arrow::Status open() = 0;

    /**
     * Convert the arrow array into a starrocks chunk via the column converter, and writes elements from the
     * specified chunk to this rowset via TabletWriter.
     */
    virtual arrow::Status write(const ArrowArray* c_arrow_array) = 0;

    /**
     * Flushes this writer and forces any buffered bytes to be written out to segment files via TabletWriter.
     */
    virtual arrow::Status flush() = 0;

    /**
     * Finish the write processing and save transaction log. This method should be called at the end of data processing.
     */
    virtual arrow::Status finish() = 0;

    /**
     * Close this writer. This method is called at the very end of the operator's life, both in the case of a successful
     * completion of the operation, and in the case of a failure and canceling.
     */
    virtual void close() = 0;

public:
    StarRocksFormatWriter(StarRocksFormatWriter&&) = delete;

    StarRocksFormatWriter& operator=(StarRocksFormatWriter&&) = delete;

protected:
    StarRocksFormatWriter() = default;
};

} // namespace starrocks::lake::format