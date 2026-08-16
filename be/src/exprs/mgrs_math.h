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
//
// ──────────────────────────────────────────────────────────────────────────────
// Self-contained UTM / MGRS math on the WGS84 ellipsoid.
//
// Ported from ClickHouse's UTMCoordinates.h/.cpp (Apache 2.0,
// © ClickHouse contributors). Adaptations for StarRocks:
//   - ClickHouse types (Float64, UInt8, String) → standard C++ (double, uint8_t,
//     std::string)
//   - All throw-on-error paths replaced with bool return codes (false = invalid)
//   - Combined into a single header-only translation unit (anonymous namespace)
// ──────────────────────────────────────────────────────────────────────────────

#pragma once

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <string>
#include <string_view>

namespace starrocks {
namespace mgrs_detail {

// ── WGS84 / UTM constants ────────────────────────────────────────────────────

static constexpr double WGS84_A = 6378137.0;
static constexpr double WGS84_ECC_SQ = 0.0066943799901413165;
static constexpr double UTM_SCALE = 0.9996;
static constexpr double FALSE_EASTING = 500000.0;
static constexpr double FALSE_NORTHING = 10000000.0;
static constexpr double DEG_TO_RAD = M_PI / 180.0;
static constexpr double RAD_TO_DEG = 180.0 / M_PI;

// MGRS latitude bands C–X (skipping I, O); X covers 72°–84°.
static constexpr std::string_view BAND_LETTERS = "CDEFGHJKLMNPQRSTUVWX";

static constexpr int NUM_100K_SETS = 6;
static constexpr std::string_view SET_ORIGIN_COLUMN_LETTERS = "AJSAJS";
static constexpr std::string_view SET_ORIGIN_ROW_LETTERS = "AFAFAF";

static constexpr int MGRS_MGRS_A_ASCII = 'A';
static constexpr int MGRS_MGRS_I_ASCII = 'I';
static constexpr int MGRS_MGRS_O_ASCII = 'O';
static constexpr int MGRS_MGRS_V_ASCII = 'V';
static constexpr int MGRS_MGRS_Z_ASCII = 'Z';

// ── Helpers ──────────────────────────────────────────────────────────────────

inline int get100kSetForZone(int zone) {
    int set = zone % NUM_100K_SETS;
    return set == 0 ? NUM_100K_SETS : set;
}

inline std::string getLetter100kID(int column, int row, int set) {
    const int index = set - 1;
    int col = SET_ORIGIN_COLUMN_LETTERS[index] + column - 1;
    int row_letter = SET_ORIGIN_ROW_LETTERS[index] + row;
    bool rollover = false;

    if (col > MGRS_Z_ASCII) {
        col = col - MGRS_Z_ASCII + MGRS_A_ASCII - 1;
        rollover = true;
    }
    if (col == MGRS_I_ASCII || (SET_ORIGIN_COLUMN_LETTERS[index] < MGRS_I_ASCII && col > MGRS_I_ASCII) ||
        ((col > MGRS_I_ASCII || SET_ORIGIN_COLUMN_LETTERS[index] < MGRS_I_ASCII) && rollover))
        ++col;
    if (col == MGRS_O_ASCII || (SET_ORIGIN_COLUMN_LETTERS[index] < MGRS_O_ASCII && col > MGRS_O_ASCII) ||
        ((col > MGRS_O_ASCII || SET_ORIGIN_COLUMN_LETTERS[index] < MGRS_O_ASCII) && rollover)) {
        ++col;
        if (col == MGRS_I_ASCII) ++col;
    }
    if (col > MGRS_Z_ASCII) col = col - MGRS_Z_ASCII + MGRS_A_ASCII - 1;

    if (row_letter > MGRS_V_ASCII) {
        row_letter = row_letter - MGRS_V_ASCII + MGRS_A_ASCII - 1;
        rollover = true;
    } else
        rollover = false;

    if (row_letter == MGRS_I_ASCII || (SET_ORIGIN_ROW_LETTERS[index] < MGRS_I_ASCII && row_letter > MGRS_I_ASCII) ||
        ((row_letter > MGRS_I_ASCII || SET_ORIGIN_ROW_LETTERS[index] < MGRS_I_ASCII) && rollover))
        ++row_letter;
    if (row_letter == MGRS_O_ASCII || (SET_ORIGIN_ROW_LETTERS[index] < MGRS_O_ASCII && row_letter > MGRS_O_ASCII) ||
        ((row_letter > MGRS_O_ASCII || SET_ORIGIN_ROW_LETTERS[index] < MGRS_O_ASCII) && rollover)) {
        ++row_letter;
        if (row_letter == MGRS_I_ASCII) ++row_letter;
    }
    if (row_letter > MGRS_V_ASCII) row_letter = row_letter - MGRS_V_ASCII + MGRS_A_ASCII - 1;

    std::string result;
    result += static_cast<char>(col);
    result += static_cast<char>(row_letter);
    return result;
}

inline std::string get100kID(double easting, double northing, int zone) {
    const int set = get100kSetForZone(zone);
    const int set_col = static_cast<int>(std::floor(easting / 100000.0));
    const int set_row = static_cast<int>(std::floor(northing / 100000.0)) % 20;
    return getLetter100kID(set_col, set_row, set);
}

inline double getMinNorthing(char band) {
    switch (band) {
    case 'C':
        return 1100000.0;
    case 'D':
        return 2000000.0;
    case 'E':
        return 2800000.0;
    case 'F':
        return 3700000.0;
    case 'G':
        return 4600000.0;
    case 'H':
        return 5500000.0;
    case 'J':
        return 6400000.0;
    case 'K':
        return 7300000.0;
    case 'L':
        return 8200000.0;
    case 'M':
        return 9100000.0;
    case 'N':
        return 0.0;
    case 'P':
        return 800000.0;
    case 'Q':
        return 1700000.0;
    case 'R':
        return 2600000.0;
    case 'S':
        return 3500000.0;
    case 'T':
        return 4400000.0;
    case 'U':
        return 5300000.0;
    case 'V':
        return 6200000.0;
    case 'W':
        return 7000000.0;
    case 'X':
        return 7900000.0;
    default:
        return -1.0; // invalid
    }
}

inline bool getEastingFromChar(char letter, int set, double& easting_out) {
    int cur = SET_ORIGIN_COLUMN_LETTERS[set - 1];
    double easting = 100000.0;
    bool rewound = false;
    while (cur != static_cast<int>(letter)) {
        ++cur;
        if (cur == MGRS_I_ASCII) ++cur;
        if (cur == MGRS_O_ASCII) ++cur;
        if (cur > MGRS_Z_ASCII) {
            if (rewound) return false;
            cur = MGRS_A_ASCII;
            rewound = true;
        }
        easting += 100000.0;
    }
    if (easting > 800000.0) return false;
    easting_out = easting;
    return true;
}

inline bool getNorthingFromChar(char letter, int set, double& northing_out) {
    if (letter > 'V') return false;
    int cur = SET_ORIGIN_ROW_LETTERS[set - 1];
    double northing = 0.0;
    bool rewound = false;
    while (cur != static_cast<int>(letter)) {
        ++cur;
        if (cur == MGRS_I_ASCII) ++cur;
        if (cur == MGRS_O_ASCII) ++cur;
        if (cur > MGRS_V_ASCII) {
            if (rewound) return false;
            cur = MGRS_A_ASCII;
            rewound = true;
        }
        northing += 100000.0;
    }
    northing_out = northing;
    return true;
}

// ── Public API ───────────────────────────────────────────────────────────────

// Returns the MGRS latitude band letter for a latitude in degrees,
// or '\0' if outside the UTM domain [-80, 84].
inline char utmLatitudeBand(double latitude) {
    if (latitude >= 72.0 && latitude <= 84.0) return 'X';
    if (latitude >= -80.0 && latitude < 72.0)
        return BAND_LETTERS[static_cast<size_t>(std::floor((latitude + 80.0) / 8.0))];
    return '\0';
}

// Converts WGS84 (longitude, latitude) in degrees to UTM.
// Returns false if lat/lon are outside the UTM domain.
// On success fills E, N (metres), zone (1–60), band letter.
inline bool wgs84ToUTM(double longitude, double latitude, double& E, double& N, int& zone, char& band) {
    band = utmLatitudeBand(latitude);
    if (band == '\0') return false;
    if (longitude < -180.0 || longitude > 180.0) return false;

    const double lat_rad = latitude * DEG_TO_RAD;
    const double lon_rad = longitude * DEG_TO_RAD;

    zone = std::clamp(static_cast<int>(std::floor((longitude + 180.0) / 6.0)) + 1, 1, 60);

    // Western Norway exception
    if (latitude >= 56.0 && latitude < 64.0 && longitude >= 3.0 && longitude < 12.0) zone = 32;

    // Svalbard exceptions
    if (latitude >= 72.0 && latitude <= 84.0) {
        if (longitude >= 0.0 && longitude < 9.0)
            zone = 31;
        else if (longitude >= 9.0 && longitude < 21.0)
            zone = 33;
        else if (longitude >= 21.0 && longitude < 33.0)
            zone = 35;
        else if (longitude >= 33.0 && longitude < 42.0)
            zone = 37;
    }

    const double lon_origin_rad = ((zone - 1) * 6.0 - 180.0 + 3.0) * DEG_TO_RAD;
    const double ecc_prime_sq = WGS84_ECC_SQ / (1.0 - WGS84_ECC_SQ);
    const double sin_lat = std::sin(lat_rad);
    const double cos_lat = std::cos(lat_rad);
    const double tan_lat = std::tan(lat_rad);
    const double n = WGS84_A / std::sqrt(1.0 - WGS84_ECC_SQ * sin_lat * sin_lat);
    const double t = tan_lat * tan_lat;
    const double c = ecc_prime_sq * cos_lat * cos_lat;
    const double a = cos_lat * (lon_rad - lon_origin_rad);
    const double e2 = WGS84_ECC_SQ;
    const double m = WGS84_A *
                     ((1.0 - e2 / 4.0 - 3.0 * e2 * e2 / 64.0 - 5.0 * e2 * e2 * e2 / 256.0) * lat_rad -
                      (3.0 * e2 / 8.0 + 3.0 * e2 * e2 / 32.0 + 45.0 * e2 * e2 * e2 / 1024.0) * std::sin(2.0 * lat_rad) +
                      (15.0 * e2 * e2 / 256.0 + 45.0 * e2 * e2 * e2 / 1024.0) * std::sin(4.0 * lat_rad) -
                      (35.0 * e2 * e2 * e2 / 3072.0) * std::sin(6.0 * lat_rad));

    E = UTM_SCALE * n *
                (a + (1.0 - t + c) * a * a * a / 6.0 +
                 (5.0 - 18.0 * t + t * t + 72.0 * c - 58.0 * ecc_prime_sq) * a * a * a * a * a / 120.0) +
        FALSE_EASTING;

    N = UTM_SCALE *
        (m + n * tan_lat *
                     (a * a / 2.0 + (5.0 - t + 9.0 * c + 4.0 * c * c) * a * a * a * a / 24.0 +
                      (61.0 - 58.0 * t + t * t + 600.0 * c - 330.0 * ecc_prime_sq) * a * a * a * a * a * a / 720.0));

    if (latitude < 0.0) N += FALSE_NORTHING;
    return true;
}

// Converts UTM back to WGS84 (longitude, latitude) in degrees.
inline bool utmToWGS84(double easting, double northing, int zone, bool is_north, double& longitude, double& latitude) {
    const double e2 = WGS84_ECC_SQ;
    const double e1 = (1.0 - std::sqrt(1.0 - e2)) / (1.0 + std::sqrt(1.0 - e2));
    const double x = easting - FALSE_EASTING;
    double y = northing;
    if (!is_north) y -= FALSE_NORTHING;

    const double lon_origin = (zone - 1) * 6.0 - 180.0 + 3.0;
    const double ecc_prime_sq = e2 / (1.0 - e2);
    const double m = y / UTM_SCALE;
    const double mu = m / (WGS84_A * (1.0 - e2 / 4.0 - 3.0 * e2 * e2 / 64.0 - 5.0 * e2 * e2 * e2 / 256.0));

    const double phi1 = mu + (3.0 * e1 / 2.0 - 27.0 * e1 * e1 * e1 / 32.0) * std::sin(2.0 * mu) +
                        (21.0 * e1 * e1 / 16.0 - 55.0 * e1 * e1 * e1 * e1 / 32.0) * std::sin(4.0 * mu) +
                        (151.0 * e1 * e1 * e1 / 96.0) * std::sin(6.0 * mu);

    const double sin_phi1 = std::sin(phi1);
    const double cos_phi1 = std::cos(phi1);
    const double tan_phi1 = std::tan(phi1);
    const double n1 = WGS84_A / std::sqrt(1.0 - e2 * sin_phi1 * sin_phi1);
    const double t1 = tan_phi1 * tan_phi1;
    const double c1 = ecc_prime_sq * cos_phi1 * cos_phi1;
    const double r1 = WGS84_A * (1.0 - e2) / std::pow(1.0 - e2 * sin_phi1 * sin_phi1, 1.5);
    const double d = x / (n1 * UTM_SCALE);

    const double lat_rad =
            phi1 - (n1 * tan_phi1 / r1) *
                           (d * d / 2.0 -
                            (5.0 + 3.0 * t1 + 10.0 * c1 - 4.0 * c1 * c1 - 9.0 * ecc_prime_sq) * d * d * d * d / 24.0 +
                            (61.0 + 90.0 * t1 + 298.0 * c1 + 45.0 * t1 * t1 - 252.0 * ecc_prime_sq - 3.0 * c1 * c1) *
                                    d * d * d * d * d * d / 720.0);
    latitude = lat_rad * RAD_TO_DEG;

    const double lon_rad = (d - (1.0 + 2.0 * t1 + c1) * d * d * d / 6.0 +
                            (5.0 - 2.0 * c1 + 28.0 * t1 - 3.0 * c1 * c1 + 8.0 * ecc_prime_sq + 24.0 * t1 * t1) * d * d *
                                    d * d * d / 120.0) /
                           cos_phi1;
    longitude = lon_origin + lon_rad * RAD_TO_DEG;
    return true;
}

// Encodes a pre-validated (lon, lat) point as an MGRS string with precision 0–5.
// Caller must have already validated that lat ∈ [-80, 84] and lon ∈ [-180, 180].
inline std::string mgrsEncode(double longitude, double latitude, uint8_t precision) {
    precision = std::min<uint8_t>(precision, 5);
    double E, N;
    int zone;
    char band;
    if (!wgs84ToUTM(longitude, latitude, E, N, zone, band)) return {};

    const int64_t E_int = static_cast<int64_t>(std::floor(E));
    const int64_t N_int = static_cast<int64_t>(std::floor(N));

    std::string result = std::to_string(zone);
    result += band;
    result += get100kID(static_cast<double>(E_int), static_cast<double>(N_int), zone);

    if (precision > 0) {
        auto most_significant = [&](int64_t value) -> std::string {
            int64_t within = value % 100000;
            if (within < 0) within += 100000;
            char buf[5];
            for (int i = 4; i >= 0; --i) {
                buf[i] = static_cast<char>('0' + within % 10);
                within /= 10;
            }
            return std::string(buf, buf + precision);
        };
        result += most_significant(E_int);
        result += most_significant(N_int);
    }
    return result;
}

// Decodes an MGRS string to WGS84 (longitude, latitude) of the grid-square centre.
// Returns false on any malformed input; whitespace is ignored, letters case-insensitive.
inline bool mgrsDecode(std::string_view mgrs, double& longitude, double& latitude) {
    std::string clean;
    clean.reserve(mgrs.size());
    for (char c : mgrs) {
        if (std::isspace(static_cast<unsigned char>(c))) continue;
        clean += static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }

    size_t i = 0;
    std::string zone_digits;
    while (i < clean.size() && std::isdigit(static_cast<unsigned char>(clean[i]))) {
        zone_digits += clean[i++];
        if (zone_digits.size() > 2) return false;
    }
    if (zone_digits.empty()) return false;

    const int zone = std::stoi(zone_digits);
    if (zone < 1 || zone > 60) return false;

    if (i + 3 > clean.size()) return false;

    const char band = clean[i++];
    if (band < 'C' || band > 'X' || band == 'I' || band == 'O') return false;
    if (band == 'X' && (zone == 32 || zone == 34 || zone == 36)) return false;

    const char col_letter = clean[i++];
    const char row_letter = clean[i++];

    const int set = get100kSetForZone(zone);
    double easting, northing;
    if (!getEastingFromChar(col_letter, set, easting)) return false;
    if (!getNorthingFromChar(row_letter, set, northing)) return false;

    const double min_northing = getMinNorthing(band);
    if (min_northing < 0.0) return false;
    while (northing < min_northing) northing += 2000000.0;

    const size_t remainder = clean.size() - i;
    if (remainder % 2 != 0) return false;
    const size_t per = remainder / 2;
    if (per > 5) return false;

    double cell_size = 100000.0;
    if (per > 0) {
        for (size_t k = i; k < clean.size(); ++k)
            if (!std::isdigit(static_cast<unsigned char>(clean[k]))) return false;
        cell_size = 100000.0 / std::pow(10.0, static_cast<double>(per));
        const std::string east_str(clean.data() + i, per);
        const std::string north_str(clean.data() + i + per, per);
        easting += std::stod(east_str) * cell_size;
        northing += std::stod(north_str) * cell_size;
    }

    // Return centre of the referenced square.
    easting += cell_size / 2.0;
    northing += cell_size / 2.0;

    const bool is_north = band >= 'N';
    if (!utmToWGS84(easting, northing, static_cast<uint8_t>(zone), is_north, longitude, latitude)) return false;

    // Validate that decoded latitude falls within the declared band (±1° tolerance).
    const size_t band_index = BAND_LETTERS.find(band);
    if (band_index == std::string_view::npos) return false;
    const double band_min = -80.0 + 8.0 * static_cast<double>(band_index);
    const double band_max = (band == 'X') ? 84.0 : band_min + 8.0;
    if (latitude < band_min - 1.0 || latitude > band_max + 1.0) return false;

    return true;
}

} // namespace mgrs_detail
} // namespace starrocks
