// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once
#include <boost/numeric/ublas/lu.hpp>
#include <boost/numeric/ublas/matrix.hpp>
#include <iomanip>
#include <numeric>
#include <sstream>

namespace starrocks {

namespace ublas = boost::numeric::ublas;

class MathHelpers {
public:
    // LU decomposition to invert a matrix
    template <class T>
    static bool invert_matrix(ublas::matrix<T> const& input, ublas::matrix<T>& inverse) {
        std::vector<size_t> nan_index;
        return invert_matrix(input, inverse, nan_index);
    }

    // LU decomposition to invert a matrix
    template <class T>
    static bool invert_matrix(const ublas::matrix<T>& input, ublas::matrix<T>& inverse,
                              std::vector<size_t>& nan_index) {
        std::vector<size_t> index;
        index.resize(input.size1());
        std::iota(index.begin(), index.end(), 0);
        using pmatrix = ublas::permutation_matrix<std::size_t>;

        ublas::matrix<double> m = input;
        while (m.size1() > 0) {
            ublas::matrix<T> l_ma(m);
            pmatrix pm(l_ma.size1());
            auto res = lu_factorize(l_ma, pm);
            if (!res) {
                break;
            }

            auto collinear_indexs = qr_decomposition(m);
            if (collinear_indexs.empty()) {
                break;
            }
            for (const auto& collinear_index : collinear_indexs) {
                nan_index.push_back(index[collinear_index]);
                index.erase(index.begin() + collinear_index);
            }

            ublas::matrix<T> new_input(index.size(), index.size());
            for (size_t i = 0; i < index.size(); i++) {
                for (size_t j = 0; j < index.size(); j++) {
                    new_input(i, j) = input(index[i], index[j]);
                }
            }
            m = new_input;
        }

        try {
            ublas::matrix<T> l_ma(m);
            pmatrix pm(l_ma.size1());
            auto res = ublas::lu_factorize(l_ma, pm);
            if (res != 0) {
                return false;
            }
            inverse = ublas::matrix<double>(l_ma.size1(), l_ma.size2());
            inverse.assign(ublas::identity_matrix<T>(l_ma.size1()));
            ublas::lu_substitute(l_ma, pm, inverse);
            ublas::matrix<double> inverse_new(input.size1(), input.size2(), std::numeric_limits<T>::quiet_NaN());
            if (input.size1() != nan_index.size() + inverse.size1()) {
                return false;
            }

            size_t num_i = 0;
            for (size_t i = 0; i < input.size1(); i++) {
                if (std::find(nan_index.begin(), nan_index.end(), i) != nan_index.end()) {
                    continue;
                }
                size_t num_j = 0;
                for (size_t j = 0; j < input.size2(); j++) {
                    if (std::find(nan_index.begin(), nan_index.end(), j) != nan_index.end()) {
                        continue;
                    }
                    inverse_new(i, j) = inverse(num_i, num_j);
                    num_j++;
                }
                num_i++;
            }
            inverse = inverse_new;
        } catch (...) {
            return false;
        }
        return true;
    }

    static std::string debug_matrix(ublas::matrix<double> const& matrix) {
        std::stringstream ss;
        ss << std::fixed << std::setprecision(2);
        ss << "[";
        for (uint32_t i = 0; i < matrix.size1(); ++i) {
            ss << "[";
            for (uint32_t j = 0; j < matrix.size2(); ++j) {
                ss << matrix(i, j);
                if (j != matrix.size2() - 1) {
                    ss << ",";
                }
            }
            ss << "]";
            if (i != matrix.size1() - 1) {
                ss << ",";
            }
        }
        ss << ']';
        return ss.str();
    }

    template <bool Scientific = true, bool remove_fractional_zero = false, typename T>
    static std::string to_string_with_precision(const T& value, int32_t len = 12, int32_t precision = 6) {
        std::ostringstream ss;
        if constexpr (std::is_floating_point<T>::value) {
            if (precision > 0) {
                ss << std::fixed << std::setprecision(precision);
            }
            ss << value;
            std::string temp = ss.str();
            if (Scientific && ss.str().size() > len) {
                ss.str("");
                ss << std::scientific << std::setprecision(4) << value;
            } else if (remove_fractional_zero) {
                while (temp.size() > 1 && temp.back() == '0') {
                    temp.pop_back();
                }
                if (temp.back() == '.') {
                    temp.pop_back();
                }
                ss.str("");
                ss << temp;
            }
        } else {
            ss << value;
        }
        std::string str = ss.str();
        if (str.size() < len) {
            str.resize(len, ' ');
        }
        if (!str.empty() && str.back() != ' ') {
            str.push_back(' ');
        }
        return str;
    }

    class MurmurHash3 {
    public:
        using hash_type = int32_t;
        explicit MurmurHash3(const uint32_t& seed = 0) : _seed(seed) {}

        // use murmurhash x86 32bit
        std::make_unsigned_t<hash_type> operator()(const hash_type& key) const {
            size_t len = sizeof(key);
            const auto* data = reinterpret_cast<const uint8_t*>(&key);
            const int nblocks = len / 4;
            int i;
            uint32_t h1 = _seed;
            uint32_t c1 = 0xcc9e2d51;
            uint32_t c2 = 0x1b873593;
            const auto* blocks = reinterpret_cast<const uint32_t*>(data + nblocks * 4);
            for (i = -nblocks; i; i++) {
                uint32_t k1 = blocks[i];
                k1 *= c1;
                k1 = rotl32(k1, 15);
                k1 *= c2;
                h1 ^= k1;
                h1 = rotl32(h1, 13);
                h1 = h1 * 5 + 0xe6546b64;
            }
            const auto* tail = static_cast<const uint8_t*>(data + nblocks * 4);
            uint32_t k1 = 0;
            switch (len & 3) {
            case 3:
                k1 ^= tail[2] << 16;
                [[fallthrough]];
            case 2:
                k1 ^= tail[1] << 8;
                [[fallthrough]];
            case 1:
                k1 ^= tail[0];
                k1 *= c1;
                k1 = rotl32(k1, 15);
                k1 *= c2;
                h1 ^= k1;
            }
            h1 ^= len;
            h1 ^= h1 >> 16;
            h1 *= 0x85ebca6b;
            h1 ^= h1 >> 13;
            h1 *= 0xc2b2ae35;
            h1 ^= h1 >> 16;
            return h1;
        }

    private:
        static uint32_t rotl32(uint32_t x, int8_t r) { return (x << r) | (x >> (32 - r)); }

        uint32_t _seed{0};
    };

    struct TupleHash {
        template <typename... TYPES>
        size_t operator()(std::tuple<TYPES...> const& tuple) const {
            size_t hash_value = 0;
            std::apply(
                    [&](const auto&... element) {
                        ((hash_value ^=
                          std::hash<std::remove_const_t<std::remove_reference_t<decltype(element)>>>()(element)),
                         ...);
                    },
                    tuple);
            return hash_value;
        }
    };

private:
    // Function to perform QR decomposition
    template <typename T>
    static std::vector<size_t> qr_decomposition(ublas::matrix<T> ma) {
        std::vector<std::vector<T>> q, r, a;
        a.resize(ma.size1(), std::vector<T>(ma.size2()));
        for (size_t i = 0; i < ma.size1(); i++) {
            for (size_t j = 0; j < ma.size2(); j++) {
                a[i][j] = ma(i, j);
            }
        }

        q = a;
        r = std::vector<std::vector<T>>(a.size(), std::vector<T>(a.size(), 0));

        for (size_t i = 0; i < a.size(); i++) {
            for (size_t j = 0; j < i; j++) {
                r[j][i] = dot_prod(q[j], a[i]);
                q[i] = subtract(q[i], multiply(q[j], r[j][i]));
            }
            r[i][i] = norm(q[i]);
            q[i] = normalize(q[i]);
        }

        std::vector<size_t> nan_index;
        for (size_t i = 0; i < r.size(); i++) {
            if (fabs(r[i][i]) < 1e-6) nan_index.push_back(i);
        }
        reverse(nan_index.begin(), nan_index.end());
        return nan_index;
    }

    template <typename T>
    static double dot_prod(const std::vector<T>& lhs, const std::vector<T>& rhs) {
        T sum = 0;
        for (size_t i = 0; i < lhs.size(); i++) {
            sum += lhs[i] * rhs[i];
        }
        return sum;
    }

    // Function to subtract vectors
    template <typename T>
    static std::vector<double> subtract(const std::vector<T>& lhs, const std::vector<T>& rhs) {
        std::vector<T> result(lhs.size());
        for (size_t i = 0; i < lhs.size(); i++) {
            result[i] = lhs[i] - rhs[i];
        }
        return result;
    }

    // Function to multiply vector by scalar
    template <typename T>
    static std::vector<double> multiply(const std::vector<T>& lhs, T rhs) {
        std::vector<T> result(lhs.size());
        for (size_t i = 0; i < lhs.size(); i++) {
            result[i] = lhs[i] * rhs;
        }
        return result;
    }

    // Function to calculate norm of vec vector
    template <typename T>
    static double norm(const std::vector<T>& vec) {
        return std::sqrt(dot_prod(vec, vec));
    }

    // Function to normalize vec vector
    template <typename T>
    static std::vector<double> normalize(const std::vector<T>& vec) {
        return multiply(vec, 1.0 / norm(vec));
    }
};

} // namespace starrocks
