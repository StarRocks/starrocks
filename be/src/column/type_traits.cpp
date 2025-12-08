#include "column/type_traits.h"

#include <velocypack/Slice.h>

namespace starrocks {

JsonValue RunTimeTypeLimits<TYPE_JSON>::min_value() {
    return JsonValue{vpack::Slice::minKeySlice()};
}

JsonValue RunTimeTypeLimits<TYPE_JSON>::max_value() {
    return JsonValue{vpack::Slice::maxKeySlice()};
}

} // namespace starrocks