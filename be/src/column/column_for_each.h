#pragma once

#include "column/column_visitor_adapter.h"
#include "common/status.h"

namespace starrocks {
template <typename Func>
concept IntCallable = std::invocable<Func, int>&& std::invocable<Func, int64_t>&& std::invocable<Func, size_t>&&
        std::invocable<Func, int8_t>&& std::invocable<Func, int16_t>;

template <class Applier>
class ForEachElementVisitor final : public ColumnVisitorAdapter<ForEachElementVisitor<Applier>> {
public:
    Applier applier;
    template <class ColumnType>
    Status do_visit(const ColumnType& _) {
        return Status::NotSupported("not supported");
    }
};

} // namespace starrocks