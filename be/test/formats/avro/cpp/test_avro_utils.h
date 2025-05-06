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

#include <chrono>
#include <sstream>

#include "formats/avro/cpp/column_reader.h"

namespace starrocks::avrocpp {

class ColumnReaderTest {
protected:
    ColumnReaderUniquePtr get_column_reader(const TypeDescriptor& type_desc, bool invalid_as_null) {
        return ColumnReader::get_nullable_column_reader(_col_name, type_desc, _timezone, invalid_as_null);
    }

    ColumnPtr create_adaptive_nullable_column(const TypeDescriptor& type_desc) {
        return ColumnHelper::create_column(type_desc, true, false, 0, true);
    }

    int days_since_epoch(const std::string& date_str) {
        std::tm tm = {};
        std::istringstream ss(date_str);

        ss >> std::get_time(&tm, "%Y-%m-%d");

        auto tp = std::chrono::system_clock::from_time_t(std::mktime(&tm));
        auto days = std::chrono::duration_cast<std::chrono::days>(tp.time_since_epoch());
        return days.count();
    }

    int64_t milliseconds_since_epoch(const std::string& datetime_str) {
        std::tm tm = {};
        char dot = '\0';
        int milliseconds = 0;

        std::istringstream ss(datetime_str);
        ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
        ss >> dot >> milliseconds;

        time_t seconds = timegm(&tm);
        return static_cast<int64_t>(seconds) * 1000 + milliseconds;
    }

    int64_t microseconds_since_epoch(const std::string& datetime_str) {
        std::tm tm = {};
        char dot = '\0';
        int microseconds = 0;

        std::istringstream ss(datetime_str);
        ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
        ss >> dot;

        std::string micro_str;
        ss >> micro_str;
        while (micro_str.size() < 6) {
            micro_str += "0";
        }
        microseconds = std::stoi(micro_str.substr(0, 6));

        time_t seconds = timegm(&tm);
        return static_cast<int64_t>(seconds) * 1000000 + microseconds;
    }

    std::vector<uint8_t> encode_decimal_bytes(int64_t unscaled_value, size_t fixed_size = 0) {
        bool is_negative = unscaled_value < 0;

        size_t bytes_size = fixed_size == 0 ? 8 : fixed_size;
        std::vector<uint8_t> result(bytes_size);

        for (size_t i = 0; i < bytes_size; ++i) {
            result[bytes_size - 1 - i] = static_cast<uint8_t>(unscaled_value & 0xFF);
            unscaled_value >>= 8;
        }

        if (fixed_size == 0) {
            // remove 0x00 or oxFF prefix
            size_t i = 0;
            while (i + 1 < result.size()) {
                if (is_negative && result[i] == 0xFF && (result[i + 1] & 0x80)) {
                    ++i;
                } else if (!is_negative && result[i] == 0x00 && !(result[i + 1] & 0x80)) {
                    ++i;
                } else {
                    break;
                }
            }

            return std::vector<uint8_t>(result.begin() + i, result.end());
        } else {
            return result;
        }
    }

    std::string _col_name = "k1";
    cctz::time_zone _timezone = cctz::utc_time_zone();
};

} // namespace starrocks::avrocpp
