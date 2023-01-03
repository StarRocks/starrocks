// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "star_cache/block_file.h"

#include "common/logging.h"
#include "io/io_error.h"
#include "star_cache/types.h"
#include "star_cache/util.h"

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
        return io::io_error(_file_path, errno);
    }

    if (pre_allocate) {
        if (::ftruncate(_fd, _quota_bytes) != 0) {
            return io::io_error(_file_path, errno);
        }
    }
    return Status::OK();
}

Status BlockFile::close() {
    if (::close(_fd) != 0) {
        return io::io_error(_file_path, errno);
    }
    return Status::OK();
}

[[maybe_unused]] static void deleter(void* buf) {
    delete[] reinterpret_cast<char*>(buf);
}

Status BlockFile::write(off_t offset, const IOBuf& buf) {
    ssize_t ret = 0;
    size_t block_num = buf.backing_block_num();

    if (block_num == 1) {
        auto data = buf.backing_block(0).data();
        if (mem_need_align(data)) {
            void* aligned_data = align_buf(buf);
            ret = ::pwrite(_fd, aligned_data, buf.size(), offset); 
            free(aligned_data);
        } else {
            ret = ::pwrite(_fd, buf.backing_block(0).data(), buf.size(), offset); 
        }
    } else if (!config::FLAGS_enable_os_page_cache) {
        void* aligned_data = align_buf(buf);
        ret = ::pwrite(_fd, aligned_data, buf.size(), offset); 
        free(aligned_data);
    } else {
        struct iovec iov[block_num];
        for (size_t i = 0; i < block_num; ++i) {
            iov[i] = { (void*)buf.backing_block(i).data(), buf.backing_block(i).size() };
        }
        ret = ::pwritev(_fd, iov, block_num, offset);
    }

    if (ret < 0) {
        return io::io_error(_file_path, errno);
    }
    return Status::OK();
}

Status BlockFile::read(off_t offset, size_t size, IOBuf* buf) {
    void* data = nullptr;
    if (config::FLAGS_enable_os_page_cache) {
        data = malloc(size);
    } else {
        posix_memalign(&data, config::FLAGS_io_align_unit_size, size);
    }
    int ret = ::pread(_fd, data, size, offset);
    if (ret < 0) {
        return io::io_error(_file_path, errno);
    }

    buf->append_user_data(data, size, /*deleter*/nullptr);
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
        return io::io_error(_file_path, errno);
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
        return io::io_error(_file_path, errno);
    }

    for (size_t i = 0; i < sizev.size(); ++i) {
        (*bufv)[i]->append_user_data(iov[i].iov_base, iov[i].iov_len, /*deleter*/nullptr);
    }
    return Status::OK();
}

} // namespace starrocks::starcache
