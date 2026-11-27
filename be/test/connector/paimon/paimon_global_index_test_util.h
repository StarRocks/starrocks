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

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <dlfcn.h>
#include <fmt/format.h>
#include <gtest/gtest.h>
#include <paimon/fs/file_system_factory.h>
#include <paimon/global_index/bitmap_global_index_result.h>
#include <paimon/global_index/global_indexer.h>
#include <paimon/global_index/global_indexer_factory.h>

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "base/utility/defer_op.h"
#include "connector/hive/paimon/paimon_global_index_evaluator.h"
#include "connector/hive/paimon/paimon_global_index_reader.h"

namespace starrocks::paimon_global_index_test {

class NativeIndexFiles final : public paimon::GlobalIndexFileWriter, public paimon::GlobalIndexFileReader {
public:
    NativeIndexFiles(std::shared_ptr<paimon::FileSystem> fs, std::string root)
            : _fs(std::move(fs)), _root(std::move(root)) {}

    paimon::Result<std::string> NewFileName(const std::string& prefix) const override {
        return prefix + "_" + std::to_string(_sequence++);
    }
    paimon::Result<std::unique_ptr<paimon::OutputStream>> NewOutputStream(const std::string& name) const override {
        return _fs->Create(ToPath(name), true);
    }
    paimon::Result<int64_t> GetFileSize(const std::string& name) const override {
        auto status = _fs->GetFileStatus(ToPath(name));
        if (!status.ok()) {
            return status.status();
        }
        return status.value()->GetLen();
    }
    std::string ToPath(const std::string& name) const override { return _root + "/" + name; }
    paimon::Result<std::unique_ptr<paimon::InputStream>> GetInputStream(const std::string& path) const override {
        return _fs->Open(path);
    }

private:
    std::shared_ptr<paimon::FileSystem> _fs;
    std::string _root;
    mutable int64_t _sequence = 0;
};

class FakeGlobalIndexReader final : public paimon::GlobalIndexReader {
public:
    explicit FakeGlobalIndexReader(std::shared_ptr<paimon::GlobalIndexResult> result) : _result(std::move(result)) {}

    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitIsNotNull() override {
        return record("is_not_null");
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitIsNull() override { return record("is_null"); }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitEqual(const paimon::Literal&) override {
        return record("equal");
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitNotEqual(const paimon::Literal&) override {
        return record("not_equal");
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitLessThan(const paimon::Literal&) override {
        return record("less_than");
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitLessOrEqual(const paimon::Literal&) override {
        return record("less_or_equal");
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitGreaterThan(const paimon::Literal&) override {
        return record("greater_than");
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitGreaterOrEqual(const paimon::Literal&) override {
        return record("greater_or_equal");
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitIn(const std::vector<paimon::Literal>&) override {
        return record("in");
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitNotIn(
            const std::vector<paimon::Literal>&) override {
        return record("not_in");
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitStartsWith(const paimon::Literal&) override {
        return record("starts_with");
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitEndsWith(const paimon::Literal&) override {
        return record("ends_with");
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitContains(const paimon::Literal&) override {
        return record("contains");
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitLike(const paimon::Literal&) override {
        return record("like");
    }
    paimon::Result<std::shared_ptr<paimon::ScoredGlobalIndexResult>> VisitVectorSearch(
            const std::shared_ptr<paimon::VectorSearch>&) override {
        return paimon::Status::Invalid("unused vector search");
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> VisitFullTextSearch(
            const std::shared_ptr<paimon::FullTextSearch>&) override {
        return paimon::Status::Invalid("unused full-text search");
    }
    bool IsThreadSafe() const override { return true; }
    std::string GetIndexType() const override { return "fake"; }

    const std::string& last_operation() const { return _last_operation; }
    void fail_with(paimon::Status status) { _status = std::move(status); }
    void set_return_null(bool return_null) { _return_null = return_null; }

private:
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> record(std::string operation) {
        _last_operation = std::move(operation);
        if (!_status.ok()) {
            return _status;
        }
        if (_return_null) {
            return std::shared_ptr<paimon::GlobalIndexResult>();
        }
        return _result;
    }

    std::shared_ptr<paimon::GlobalIndexResult> _result;
    std::string _last_operation;
    paimon::Status _status;
    bool _return_null = false;
};

inline std::shared_ptr<paimon::GlobalIndexResult> result_from_ranges(std::vector<paimon::Range> ranges) {
    return paimon::BitmapGlobalIndexResult::FromRanges(ranges);
}

inline rapidjson::Document parse_json(const char* json) {
    rapidjson::Document document;
    document.Parse(json);
    EXPECT_FALSE(document.HasParseError());
    return document;
}

inline PaimonGlobalIndexEvaluator make_evaluator(PaimonGlobalIndexEvaluator::ReaderGetter reader,
                                                 std::string type = "int") {
    return PaimonGlobalIndexEvaluator(
            std::move(reader), [type = std::move(type)](std::string_view) -> StatusOr<std::string> { return type; });
}

} // namespace starrocks::paimon_global_index_test
