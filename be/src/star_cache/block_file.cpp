// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "star_cache/block_file.h"

#include "common/logging.h"
#include "io/io_error.h"
#include "util/bit_util.h"
#include "star_cache/types.h"
#include "star_cache/utils.h"
#include "star_cache/macros.h"

namespace starrocks::starcache {

Status BlockFile::open(bool pre_allocate) {
    // TODO: use direct io (the buffer memory should be aligned)
    // _fd = ::open(_file_path.c_str(), O_RDWR | O_CREAT | O_DIRECT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    int oflag = O_RDWR | O_CREAT;
    if (!config::FLAGS_enable_os_page_cache) {
        oflag |= O_DIRECT;
    }
    _fd = ::open(_file_path.c_str(), oflag, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    if (_fd < 0) {
        return _report_io_error("fail to open block file");
    }

    if (pre_allocate) {
        if (::ftruncate(_fd, _quota_bytes) != 0) {
            return _report_io_error("fail to truncate block file");
        }
    }
    LOG(INFO) << "open block file success, fd: " << _fd << ", path: " << _file_path;
    return Status::OK();
}

Status BlockFile::close() {
    if (::close(_fd) != 0) {
        return _report_io_error("fail to close block file");
    }
    return Status::OK();
}

[[maybe_unused]] static void deleter(void* buf) {
    delete[] reinterpret_cast<char*>(buf);
}

Status BlockFile::write(off_t offset, const IOBuf& buf) {
    ssize_t ret = 0;
    size_t block_num = buf.backing_block_num();

    void* aligned_data = nullptr;
    size_t aligned_size = buf.size();
    if (block_num == 1) {
        auto data = buf.backing_block(0).data();
        if (mem_need_align(data, buf.size())) {
            aligned_size = align_buf(buf, &aligned_data);
            ret = ::pwrite(_fd, aligned_data, aligned_size, offset); 
            free(aligned_data);
        } else {
            ret = ::pwrite(_fd, data, aligned_size, offset); 
        }
    } else if (!config::FLAGS_enable_os_page_cache) {
        aligned_size = align_buf(buf, &aligned_data);
        ret = ::pwrite(_fd, aligned_data, aligned_size, offset); 
        free(aligned_data);
    } else {
        struct iovec iov[block_num];
        for (size_t i = 0; i < block_num; ++i) {
            iov[i] = { (void*)buf.backing_block(i).data(), buf.backing_block(i).size() };
        }
        ret = ::pwritev(_fd, iov, block_num, offset);
    }
    if (ret < 0) {
        return _report_io_error("fail to write block file");
    }

    STAR_VLOG << "write block file success, fd: " << _fd << ", path: " << _file_path
              << ", offset: " << offset << ", buf size: " << buf.size()
              << ", aligned_size: " << aligned_size << ", buf block num: " << block_num;
    return Status::OK();
}

Status BlockFile::read(off_t offset, size_t size, IOBuf* buf) {
    void* data = nullptr;
    size_t aligned_size = size;
    if (config::FLAGS_enable_os_page_cache) {
        data = malloc(size);
    } else {
        size_t aligned_unit = config::FLAGS_io_align_unit_size;
        aligned_size = starrocks::BitUtil::round_up(size, aligned_unit);
        int ret = posix_memalign(&data, aligned_unit, aligned_size);
        if (ret != 0) {
            auto st = io::io_error(_file_path, ret);
            DCHECK(false) << "posix memalign error: " << st.get_error_msg();
            LOG(ERROR) << "posix posix_memalign error: " << st.get_error_msg();
            return st;
        }
        DCHECK(data);
    }
    int ret = ::pread(_fd, data, aligned_size, offset);
    if (ret < 0) {
        return _report_io_error("fail to read block file");
    }
    buf->append_user_data(data, aligned_size, /*deleter*/nullptr);

    STAR_VLOG << "read block file, fd: " << _fd << ", path: " << _file_path
              << ", offset: " << offset << ", size: " << size
              << ", aligned_size: " << aligned_size;
    return Status::OK();
}

Status BlockFile::writev(off_t offset, const std::vector<IOBuf*>& bufv) {
    ssize_t ret = 0;
    size_t block_num = 0;
    for (auto& buf : bufv) {
        block_num += buf->backing_block_num();
    }

    struct iovec iov[block_num];
    size_t index = 0;
    for (auto& buf : bufv) {
        for (size_t i = 0; i < buf->backing_block_num(); ++i) {
            iov[index++] = { (void*)buf->backing_block(i).data(), buf->backing_block(i).size() };
        }
    }
    ret = ::pwritev(_fd, iov, block_num, offset);
    if (ret < 0) {
        return _report_io_error("fail to writev block file");
    }
    return Status::OK();
}

Status BlockFile::readv(off_t offset, const std::vector<size_t>& sizev, std::vector<IOBuf*>* bufv) {
    struct iovec iov[sizev.size()];
    for (size_t i = 0; i < sizev.size(); ++i) {
        void* data = malloc(sizev[i]);
        iov[i] = { data, sizev[i] };
    }
    int ret = ::preadv(_fd, iov, sizev.size(), offset);
    if (ret < 0) {
        return _report_io_error("fail to readv block file");
    }

    for (size_t i = 0; i < sizev.size(); ++i) {
        (*bufv)[i]->append_user_data(iov[i].iov_base, iov[i].iov_len, /*deleter*/nullptr);
    }
    return Status::OK();
}

Status BlockFile::_report_io_error(const std::string& err_desc) {
    auto st = io::io_error(_file_path, errno);
    LOG(ERROR) << err_desc << ", reason: " << st.get_error_msg();
    return st;
}

} // namespace starrocks::starcache
