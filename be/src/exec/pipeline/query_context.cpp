// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
#include "exec/pipeline/query_context.h"

#include "exec/pipeline/fragment_context.h"

namespace starrocks {
namespace pipeline {
QueryContext::QueryContext()
        : _fragment_mgr(new FragmentContextManager()), _num_fragments(0), _num_active_fragments(0) {}

FragmentContextManager* QueryContext::fragment_mgr() {
    return _fragment_mgr.get();
}

void QueryContext::cancel(const Status& status) {
    _fragment_mgr->cancel(status);
}

QueryContextManager::QueryContextManager() {}
QueryContextManager::~QueryContextManager() {}
QueryContext* QueryContextManager::get_or_register(const TUniqueId& query_id) {
    std::lock_guard lock(_lock);
    auto iter = _contexts.find(query_id);
    if (iter != _contexts.end()) {
        iter->second->increment_num_fragments();
        return iter->second.get();
    }

    auto&& ctx = std::make_unique<QueryContext>();
    auto* ctx_raw_ptr = ctx.get();
    ctx_raw_ptr->increment_num_fragments();
    _contexts.emplace(query_id, std::move(ctx));
    return ctx_raw_ptr;
}

QueryContextPtr QueryContextManager::get(const TUniqueId& query_id) {
    std::lock_guard lock(_lock);
    auto it = _contexts.find(query_id);
    if (it != _contexts.end()) {
        return it->second;
    } else {
        return nullptr;
    }
}

QueryContextPtr QueryContextManager::remove(const TUniqueId& query_id) {
    std::lock_guard lock(_lock);
    auto it = _contexts.find(query_id);
    if (it != _contexts.end()) {
        auto&& ctx = std::move(it->second);
        _contexts.erase(it);
        return ctx;
    } else {
        return nullptr;
    }
}

void QueryContextManager::unregister(const TUniqueId& query_id) {
    std::lock_guard lock(_lock);
    _contexts.erase(query_id);
}
static inline int closest_prime(int n) {
    // prime numbers below 1000
    static int primes[] = {
            2,   3,   5,   7,   11,  13,  17,  19,  23,  29,  31,  37,  41,  43,  47,  53,  59,  61,  67,  71,  73,
            79,  83,  89,  97,  101, 103, 107, 109, 113, 127, 131, 137, 139, 149, 151, 157, 163, 167, 173, 179, 181,
            191, 193, 197, 199, 211, 223, 227, 229, 233, 239, 241, 251, 257, 263, 269, 271, 277, 281, 283, 293, 307,
            311, 313, 317, 331, 337, 347, 349, 353, 359, 367, 373, 379, 383, 389, 397, 401, 409, 419, 421, 431, 433,
            439, 443, 449, 457, 461, 463, 467, 479, 487, 491, 499, 503, 509, 521, 523, 541, 547, 557, 563, 569, 571,
            577, 587, 593, 599, 601, 607, 613, 617, 619, 631, 641, 643, 647, 653, 659, 661, 673, 677, 683, 691, 701,
            709, 719, 727, 733, 739, 743, 751, 757, 761, 769, 773, 787, 797, 809, 811, 821, 823, 827, 829, 839, 853,
            857, 859, 863, 877, 881, 883, 887, 907, 911, 919, 929, 937, 941, 947, 953, 967, 971, 977, 983, 991, 997};
    // binary search closest prime number:
    // 1. n <= 997, return minimum prime number >= n.
    // 2. n > 997, return 997.
    int l = 0;
    int h = sizeof(primes) / sizeof(primes[0]) - 1;
    while (l < h) {
        int m = (l + h) / 2;
        int p = primes[m];
        if (p == n) {
            return n;
        } else if (p < n) {
            l = m + 1;
        } else {
            h = m;
        }
    }
    return primes[h];
}

DyingQueryContextReaper::DyingQueryContextReaper(const int& size) : _size(closest_prime(size)) {
    _mutexes.reserve(_size);
    for (auto i = 0; i < _size; ++i) {
        _mutexes.emplace_back(std::make_unique<std::mutex>());
    }
    _slots.resize(_size);
}

void DyingQueryContextReaper::add(QueryContextPtr&& query_ctx) {
    DCHECK(query_ctx);
    auto i = hash_value(query_ctx->query_id()) % _size;
    std::lock_guard<std::mutex> lock(*_mutexes[i]);
    auto& target_query_ctx_list = _slots[i];
    target_query_ctx_list.emplace_back(std::move(query_ctx));
}
void DyingQueryContextReaper::add(size_t i, QueryContextList&& query_ctx_list) {
    DCHECK(0 <= i && i < _size);
    std::lock_guard<std::mutex> lock(*_mutexes[i]);
    auto& target_query_ctx_list = _slots[i];
    target_query_ctx_list.splice(target_query_ctx_list.end(), std::move(query_ctx_list));
}
std::list<QueryContextPtr> DyingQueryContextReaper::get(size_t i) {
    DCHECK(0 <= i && i < _size);
    QueryContextList query_ctx_list;
    std::lock_guard<std::mutex> lock(*_mutexes[i]);
    query_ctx_list.splice(query_ctx_list.end(), std::move(_slots[i]));
    return query_ctx_list;
}
} // namespace pipeline
} // namespace starrocks
