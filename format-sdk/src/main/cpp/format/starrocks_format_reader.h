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
#include <arrow/result.h>
#include <arrow/type.h>

namespace starrocks::lake::format {

/**
 * StarRocksFormatReader is a tablet file reader, which is used to bypass BE Server to read tablet files that stored
 * on the file systems (e.g. S3, HDFS) in share-data mode. These data files can be written by StarRocks or by the
 * StarRocksFormatWriter provided by this SDK.
 *
 * StarRocksFormatReader can be regarded as a wrapper of StarRocks TabletReader, and all file operation methods will
 * eventually be mapped to the corresponding methods in TabletReader.
 *
 * You can refer to the following examples for use:
 * @code
 * // Create and open Reader
 * auto&& result = StarRocksFormatReader::create(...);
 * StarRocksFormatReader* reader = std::move(result).ValueUnsafe();
 * reader->open();
 *
 * // Read tablet data iteratively
 * reader->get_next(arrow_array);
 *
 * // Close Reader
 * reader->close();
 */
class StarRocksFormatReader {
public:
    static arrow::Result<StarRocksFormatReader*> create(int64_t tablet_id, std::string tablet_root_path,
                                                        int64_t version, ArrowSchema* required_schema,
                                                        ArrowSchema* output_schema,
                                                        std::unordered_map<std::string, std::string> options);

    /**
     * Create a starrocks format reader instance, which bound to a specific tablet.
     *
     * @param tablet_id The target tablet id.
     * @param tablet_root_path The target tablet root path.
     * @param version The target tablet version.
     * @param required_schema The required schema, also contains the union of output fields and pushdown filter fields.
     * @param output_schema The output fields.
     * @param options key-value parameters, e.g. S3 connection authentication configuration.
     */
    static arrow::Result<StarRocksFormatReader*> create(int64_t tablet_id, std::string tablet_root_path,
                                                        int64_t version, std::shared_ptr<arrow::Schema> required_schema,
                                                        std::shared_ptr<arrow::Schema> output_schema,
                                                        std::unordered_map<std::string, std::string> options);

    virtual ~StarRocksFormatReader() = default;

    /**
     * Open the format reader, the StarRocks TabletReader will be initialized and opened during this time.
     */
    virtual arrow::Status open() = 0;

    /**
     * Get the starrocks chunk via TabletReader, and convert it to arrow array by column converter.
     */
    virtual arrow::Status get_next(ArrowArray* c_arrow_array) = 0;

    /**
     * Close this reader, the TabletReader will be closed and reset during this time.
     */
    virtual void close() = 0;

public:
    StarRocksFormatReader(StarRocksFormatReader&&) = delete;

    StarRocksFormatReader& operator=(StarRocksFormatReader&&) = delete;

protected:
    StarRocksFormatReader() = default;
};

} // namespace starrocks::lake::format
