#include "types/int256.h"

#include <glog/logging.h>

#include <algorithm> // for std::reverse

namespace starrocks {

std::string to_string(const int256_t& value) {
    LOG(ERROR) << "11111111111111";
    if (value.high == 0 && value.low == 0) {
        return "0";
    }
    int256_t temp = value;
    bool negative = false;
    if (temp.high < 0) {
        negative = true;
        temp = -temp;
    }
    std::string result;
    while (temp.high != 0 || temp.low != 0) {
        uint32_t rem;
        int256_t quot;
        divmod_u32(temp, 10, &quot, &rem);
        result.push_back('0' + rem);
        temp = quot;
    }
    if (negative) result.push_back('-');
    std::reverse(result.begin(), result.end());
    return result;
}

} // namespace starrocks