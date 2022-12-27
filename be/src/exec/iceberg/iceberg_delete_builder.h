// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <utility>

#include "column/column_helper.h"
#include "common/status.h"
#include "fs/fs.h"
#include "gutil/strings/substitute.h"
#include "runtime/descriptors.h"

namespace starrocks::vectorized {
struct IcebergColumnMeta;

class PositionDeleteBuilder {
public:
    PositionDeleteBuilder() = default;
    virtual ~PositionDeleteBuilder() = default;

    virtual Status build(const std::string& timezone, const std::string& file_path, int64_t file_length,
                         std::set<int64_t>* need_skip_rowids) = 0;
};

class ORCPositionDeleteBuilder : public PositionDeleteBuilder {
public:
    ORCPositionDeleteBuilder(FileSystem* fs, std::string datafile_path)
            : _fs(fs), _datafile_path(std::move(datafile_path)) {}
    ~ORCPositionDeleteBuilder() override = default;

    Status build(const std::string& timezone, const std::string& delete_file_path, int64_t file_length,
                 std::set<int64_t>* need_skip_rowids) override;

private:
    FileSystem* _fs;
    std::string _datafile_path;
};

class IcebergDeleteBuilder {
public:
    IcebergDeleteBuilder(FileSystem* fs, std::string datafile_path, std::vector<ExprContext*> conjunct_ctxs,
                         std::vector<SlotDescriptor*> materialize_slots, std::set<std::int64_t>* need_skip_rowids)
            : _fs(fs),
              _datafile_path(std::move(datafile_path)),
              _conjunct_ctxs(std::move(conjunct_ctxs)),
              _materialize_slots(std::move(materialize_slots)),
              _need_skip_rowids(need_skip_rowids) {}
    ~IcebergDeleteBuilder() = default;

    Status build_orc(const std::string& timezone, const TIcebergDeleteFile& delete_file) {
        if (delete_file.file_content == TIcebergFileContent::POSITION_DELETES) {
            return ORCPositionDeleteBuilder(_fs, _datafile_path)
                    .build(timezone, delete_file.full_path, delete_file.length, _need_skip_rowids);
        } else {
            auto s = strings::Substitute("Unsupported iceberg file content: $0", delete_file.file_content);
            LOG(WARNING) << s;
            return Status::InternalError(s);
        }
    }

private:
    FileSystem* _fs;
    std::string _datafile_path;
    std::vector<ExprContext*> _conjunct_ctxs;
    std::vector<SlotDescriptor*> _materialize_slots;
    std::set<std::int64_t>* _need_skip_rowids;
};

class IcebergDeleteFileMeta {
public:
    IcebergDeleteFileMeta() = default;
    ~IcebergDeleteFileMeta() = default;

    static SlotDescriptor& get_delete_file_path_slot();
    static SlotDescriptor& get_delete_file_pos_slot();

private:
    static SlotDescriptor gen_slot_helper(const IcebergColumnMeta& meta);
};
} // namespace starrocks::vectorized