// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "fs/fd_cache.h"

#include <unistd.h>

#include "util/lru_cache.h"

namespace starrocks {

static void fd_deleter(const CacheKey& key, void* value) {
    int fd = static_cast<int>(reinterpret_cast<uintptr_t>(value));
    ::close(fd);
}

FdCache::FdCache(size_t capacity) : _cache(new_lru_cache(capacity)) {}

FdCache::~FdCache() {
    delete _cache;
}

FdCache::Handle* FdCache::insert(std::string_view path, int fd) {
    void* value = reinterpret_cast<void*>(static_cast<uintptr_t>(fd));
    Cache::Handle* h = _cache->insert(CacheKey(path.data(), path.size()), value, 1, fd_deleter);
    return reinterpret_cast<FdCache::Handle*>(h);
}

FdCache::Handle* FdCache::lookup(std::string_view path) {
    Cache::Handle* h = _cache->lookup(CacheKey(path.data(), path.size()));
    return reinterpret_cast<FdCache::Handle*>(h);
}

void FdCache::erase(std::string_view path) {
    _cache->erase(CacheKey(path.data(), path.size()));
}

void FdCache::release(Handle* handle) {
    _cache->release(reinterpret_cast<Cache::Handle*>(handle));
}

void FdCache::prune() {
    _cache->prune();
}

int FdCache::fd(Handle* handle) {
    void* value = FdCache::Instance()->_cache->value(reinterpret_cast<Cache::Handle*>(handle));
    int fd = static_cast<int>(reinterpret_cast<uintptr_t>(value));
    return fd;
}

} // namespace starrocks
