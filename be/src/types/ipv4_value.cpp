// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "types/ipv4_value.h"
#include <cstring>

namespace starrocks::vectorized {

    Ipv4Value Ipv4Value::MIN_IPV4_VALUE{0};
    Ipv4Value Ipv4Value::MAX_IPV4_VALUE{4294967295};

    void swapStr(char *str, int begin, int end) {
        int i, j;
        for (i = begin, j = end; i <= j; i++, j--) {
            if (str[i] != str[j]) {
                str[i] = str[i] ^ str[j];
                str[j] = str[i] ^ str[j];
                str[i] = str[i] ^ str[j];
            }
        }
    }

    std::string Ipv4Value::to_string() const {
        uint ipint = _ip;
        char *dd = (char *) malloc(IPV4_LEN);
        memset(dd, '\0', IPV4_LEN);
        dd[0] = '.';
        char token[4];
        int bt, ed, len, cur;
        while (ipint) {
            cur = ipint % 256;
            sprintf(token, "%d", cur);
            strcat(dd, token);
            ipint /= 256;
            if (ipint) strcat(dd, ".");
        }
        len = strlen(dd);
        swapStr(dd, 0, len - 1);
        for (bt = ed = 0; ed < len;) {
            while (ed < len && dd[ed] != '.') {
                ed++;
            }
            swapStr(dd, bt, ed - 1);
            ed += 1;
            bt = ed;
        }
        dd[len - 1] = '\0';
        return dd;
//    return "192.89.8.9";
    }

    bool Ipv4Value::from_string(const char *ip_str, size_t len) {
        if (ip_str == NULL) return false;
        char *token;
        uint i = 3, total = 0, cur;
        token = strtok((char *) ip_str, ".");
        while (token != NULL) {
            cur = atoi(token);
            if (cur >= 0 && cur <= 255) {
                total += cur * pow(256, i);
            }
            i--;
            token = strtok(NULL, ".");
        }
        _ip = total;
        return true;
    }

    uint64_t Ipv4Value::to_ipv4_literal() const {
        return 33;
    }
} // namespace starrocks::vectorized
