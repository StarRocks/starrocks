//
// Created by xavier bai on 2025/5/26.
//

#pragma once

#include <utility>

#include "formats/parquet/variant.h"
#include "column/column.h"
#include "column/object_column.h"
#include "column/vectorized_fwd.h"

namespace starrocks {
class VariantColumn final : public CowFactory<ColumnFactory<ObjectColumn<Variant>, VariantColumn>, VariantColumn, Column> {
public:
    using SuperClass = CowFactory;
    using BaseClass = VariantColumnBase;

    VariantColumn() = default;
    explicit VariantColumn(size_t size) : SuperClass(size) {}
    VariantColumn(const VariantColumn& rhs) : SuperClass(rhs) {}

    VariantColumn(VariantColumn&& rhs) noexcept : SuperClass(std::move(rhs)) {

    }

    /// \brief column overrides
    bool is_variant() const override { return true; }



    /// \brief object_column overrides
    std::string get_name() const override;
};
}