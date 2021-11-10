// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/vectorized/row_source_mask.h"

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"

namespace starrocks::vectorized {

RowSourceMaskBuffer::RowSourceMaskBuffer(int64_t tablet_id, const std::string& storage_root_path)
        : _mask_column(std::move(UInt16Column::create_mutable())),
          _tablet_id(tablet_id),
          _storage_root_path(storage_root_path) {}

RowSourceMaskBuffer::~RowSourceMaskBuffer() {
    _reset_mask_column();
    if (_tmp_file_fd > 0) {
        ::close(_tmp_file_fd);
    }
}

Status RowSourceMaskBuffer::write(const std::vector<RowSourceMask>& source_masks) {
    size_t source_masks_size = source_masks.size() * sizeof(RowSourceMask);
    if (_mask_column->byte_size() + source_masks_size >= config::max_row_source_mask_memory_bytes &&
        !_mask_column->empty()) {
        if (_tmp_file_fd == -1) {
            RETURN_IF_ERROR(_create_tmp_file());
        }
        RETURN_IF_ERROR(_serialize_masks());
        _reset_mask_column();
    }

    for (const auto& mask : source_masks) {
        _mask_column->append(mask.data);
    }
    return Status::OK();
}

StatusOr<bool> RowSourceMaskBuffer::has_remaining() {
    if (_current_index < _mask_column->size()) {
        return true;
    }

    if (_tmp_file_fd > 0) {
        DCHECK_EQ(_current_index, _mask_column->size());
        _reset_mask_column();
        _current_index = 0;
        Status st = _deserialize_masks();
        if (st.ok()) {
            return true;
        } else if (!st.is_end_of_file()) {
            return st;
        }
    }
    return false;
}

bool RowSourceMaskBuffer::has_same_source(uint16_t source, size_t count) const {
    if (_mask_column->size() - _current_index < count) {
        return false;
    }

    for (int i = 0; i < count; ++i) {
        RowSourceMask mask(_mask_column->get(_current_index + i).get_uint16());
        if (mask.get_source_num() != source) {
            return false;
        }
    }
    return true;
}

Status RowSourceMaskBuffer::flip() {
    _current_index = 0;
    if (_tmp_file_fd > 0) {
        off_t offset = lseek(_tmp_file_fd, 0, SEEK_SET);
        if (offset != 0) {
            PLOG(WARNING) << "fail to seek to offset 0. offset=" << offset;
            return Status::InternalError("fail to seek to offset 0");
        }
        _reset_mask_column();
    }
    return Status::OK();
}

Status RowSourceMaskBuffer::flush() {
    if (_tmp_file_fd > 0 && !_mask_column->empty()) {
        RETURN_IF_ERROR(_serialize_masks());
        _reset_mask_column();
    }
    return Status::OK();
}

Status RowSourceMaskBuffer::_create_tmp_file() {
    std::stringstream tmp_file_path_s;
    // storage/tmp/compaction_mask_12345.abcdef
    tmp_file_path_s << _storage_root_path << TMP_PREFIX << "/"
                    << "compaction_mask_" << _tablet_id << ".XXXXXX";
    std::string tmp_file_path = tmp_file_path_s.str();
    _tmp_file_fd = mkstemp(tmp_file_path.data());
    if (_tmp_file_fd < 0) {
        PLOG(WARNING) << "fail to create mask tmp file. path=" << tmp_file_path;
        return Status::InternalError("fail to create mask tmp file");
    }
    unlink(tmp_file_path.data());
    return Status::OK();
}

Status RowSourceMaskBuffer::_serialize_masks() {
    size_t content_size = _mask_column->serialize_size();
    ssize_t w_size = ::write(_tmp_file_fd, &content_size, sizeof(uint64_t));
    if (w_size != sizeof(uint64_t)) {
        PLOG(WARNING) << "fail to write masks size to mask file. write size=" << w_size;
        return Status::InternalError("fail to write masks size to mask file");
    }

    string content;
    content.resize(content_size);
    _mask_column->serialize_column((uint8_t*)(content.data()));
    w_size = ::write(_tmp_file_fd, content.data(), content_size);
    if (w_size != content_size) {
        PLOG(WARNING) << "fail to write masks to mask file. write size=" << w_size;
        return Status::InternalError("fail to write masks to mask file");
    }
    return Status::OK();
}

Status RowSourceMaskBuffer::_deserialize_masks() {
    uint64_t content_size = 0;
    ssize_t r_size = ::read(_tmp_file_fd, &content_size, sizeof(uint64_t));
    if (r_size == 0) {
        return Status::EndOfFile("end of file");
    } else if (r_size != sizeof(uint64_t)) {
        PLOG(WARNING) << "fail to read masks size from mask file. read size=" << r_size;
        return Status::InternalError("fail to read masks size from mask file");
    }

    string content;
    content.resize(content_size);
    r_size = ::read(_tmp_file_fd, content.data(), content_size);
    if (r_size != content_size) {
        PLOG(WARNING) << "fail to read masks from mask file. read size=" << r_size;
        return Status::InternalError("fail to read masks from mask file");
    }
    _mask_column->deserialize_column((uint8_t*)(content.data()));
    return Status::OK();
}

} // namespace starrocks::vectorized