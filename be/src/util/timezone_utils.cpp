// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/timezone_utils.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

#include "util/timezone_utils.h"

#include <charconv>
#include <string_view>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "util/phmap/phmap.h"

namespace starrocks {

RE2 TimezoneUtils::time_zone_offset_format_reg(R"(^[+-]{1}\d{2}\:\d{2}$)");

const std::string TimezoneUtils::default_time_zone = "+08:00";

static phmap::flat_hash_map<std::string_view, cctz::time_zone> _s_cached_timezone;
static phmap::flat_hash_map<std::pair<std::string_view, std::string_view>, int64_t> _s_cached_offsets;

void TimezoneUtils::init_time_zones() {
    // timezone cache list
    // We cannot add a time zone to the cache that contains both daylight saving time and winter time
    static std::vector<std::string> timezones = {
        "Africa/Abidjan","Africa/Accra","Africa/Addis_Ababa","Africa/Algiers","Africa/Asmara",
        "Africa/Asmera","Africa/Bamako","Africa/Bangui","Africa/Banjul","Africa/Bissau",
        "Africa/Blantyre","Africa/Brazzaville","Africa/Bujumbura","Africa/Cairo","Africa/Conakry",
        "Africa/Dakar","Africa/Dar_es_Salaam","Africa/Djibouti","Africa/Douala","Africa/Freetown",
        "Africa/Gaborone","Africa/Harare","Africa/Johannesburg","Africa/Juba","Africa/Kampala",
        "Africa/Khartoum","Africa/Kigali","Africa/Kinshasa","Africa/Lagos","Africa/Libreville",
        "Africa/Lome","Africa/Luanda","Africa/Lubumbashi","Africa/Lusaka","Africa/Malabo",
        "Africa/Maputo","Africa/Maseru","Africa/Mbabane","Africa/Mogadishu","Africa/Monrovia",
        "Africa/Nairobi","Africa/Ndjamena","Africa/Niamey","Africa/Nouakchott","Africa/Ouagadougou",
        "Africa/Porto-Novo","Africa/Sao_Tome","Africa/Timbuktu","Africa/Tripoli","Africa/Tunis",
        "Africa/Windhoek","America/Anguilla","America/Antigua","America/Araguaina","America/Argentina/Buenos_Aires",
        "America/Argentina/Catamarca","America/Argentina/ComodRivadavia","America/Argentina/Cordoba",
        "America/Argentina/Jujuy","America/Argentina/La_Rioja","America/Argentina/Mendoza",
        "America/Argentina/Rio_Gallegos","America/Argentina/Salta","America/Argentina/San_Juan",
        "America/Argentina/San_Luis","America/Argentina/Tucuman","America/Argentina/Ushuaia",
        "America/Aruba","America/Atikokan","America/Bahia","America/Barbados","America/Belem",
        "America/Belize","America/Blanc-Sablon","America/Boa_Vista","America/Bogota","America/Buenos_Aires",
        "America/Campo_Grande","America/Cancun","America/Caracas","America/Catamarca","America/Cayenne","America/Cayman",
        "America/Coral_Harbour","America/Cordoba","America/Costa_Rica","America/Creston","America/Cuiaba","America/Curacao",
        "America/Danmarkshavn","America/Dawson","America/Dawson_Creek","America/Dominica","America/Eirunepe",
        "America/El_Salvador","America/Fort_Nelson","America/Fortaleza","America/Grenada","America/Guadeloupe",
        "America/Guatemala","America/Guayaquil","America/Guyana","America/Hermosillo","America/Jamaica","America/Jujuy",
        "America/Kralendijk","America/La_Paz","America/Lima","America/Lower_Princes","America/Maceio","America/Managua",
        "America/Manaus","America/Marigot","America/Martinique","America/Mendoza","America/Montevideo","America/Montserrat",
        "America/Noronha","America/Panama","America/Paramaribo","America/Phoenix","America/Port_of_Spain","America/Porto_Acre",
        "America/Porto_Velho","America/Puerto_Rico","America/Recife","America/Regina","America/Rio_Branco",
        "America/Rosario","America/Santarem","America/Santo_Domingo","America/Sao_Paulo","America/St_Barthelemy","America/St_Kitts",
        "America/St_Lucia","America/St_Thomas","America/St_Vincent","America/Swift_Current","America/Tegucigalpa","America/Tortola",
        "America/Virgin","America/Whitehorse","Antarctica/Casey","Antarctica/Davis","Antarctica/DumontDUrville","Antarctica/Mawson",
        "Antarctica/Palmer","Antarctica/Rothera","Antarctica/Syowa","Antarctica/Vostok","Asia/Aden","Asia/Almaty","Asia/Anadyr",
        "Asia/Aqtau","Asia/Aqtobe","Asia/Ashgabat","Asia/Ashkhabad","Asia/Atyrau","Asia/Baghdad","Asia/Bahrain","Asia/Baku",
        "Asia/Bangkok","Asia/Barnaul","Asia/Bishkek","Asia/Brunei","Asia/Calcutta","Asia/Chita","Asia/Choibalsan","Asia/Chongqing",
        "Asia/Chungking","Asia/Colombo","Asia/Dacca","Asia/Dhaka","Asia/Dili","Asia/Dubai","Asia/Dushanbe","Asia/Harbin","Asia/Ho_Chi_Minh",
        "Asia/Hong_Kong","Asia/Hovd","Asia/Irkutsk","Asia/Istanbul","Asia/Jakarta","Asia/Jayapura","Asia/Kabul","Asia/Kamchatka","Asia/Karachi",
        "Asia/Kashgar","Asia/Kathmandu","Asia/Katmandu","Asia/Khandyga","Asia/Kolkata","Asia/Krasnoyarsk","Asia/Kuala_Lumpur","Asia/Kuching",
        "Asia/Kuwait","Asia/Macao","Asia/Macau","Asia/Magadan","Asia/Makassar","Asia/Manila","Asia/Muscat","Asia/Novokuznetsk",
        "Asia/Novosibirsk","Asia/Omsk","Asia/Oral","Asia/Phnom_Penh","Asia/Pontianak","Asia/Pyongyang","Asia/Qatar",
        "Asia/Qyzylorda","Asia/Rangoon","Asia/Riyadh","Asia/Saigon","Asia/Sakhalin","Asia/Samarkand","Asia/Seoul","Asia/Shanghai",
        "Asia/Singapore","Asia/Srednekolymsk","Asia/Taipei","Asia/Tashkent","Asia/Tbilisi","Asia/Thimbu","Asia/Thimphu","Asia/Tokyo",
        "Asia/Tomsk","Asia/Ujung_Pandang","Asia/Ulaanbaatar","Asia/Ulan_Bator","Asia/Urumqi","Asia/Ust-Nera","Asia/Vientiane",
        "Asia/Vladivostok","Asia/Yakutsk","Asia/Yangon","Asia/Yekaterinburg","Asia/Yerevan","Atlantic/Cape_Verde","Atlantic/Reykjavik",
        "Atlantic/South_Georgia","Atlantic/St_Helena","Atlantic/Stanley","Australia/Brisbane","Australia/Darwin","Australia/Eucla",
        "Australia/Lindeman","Australia/North","Australia/Perth","Australia/Queensland","Australia/West","Brazil/Acre","Brazil/DeNoronha",
        "Brazil/East","Brazil/West","Canada/Saskatchewan","Canada/Yukon","Egypt","EST","Etc/GMT","Etc/GMT-0","Etc/GMT-1","Etc/GMT-2",
        "Etc/GMT-3","Etc/GMT-4","Etc/GMT-5","Etc/GMT-6","Etc/GMT-7","Etc/GMT-8","Etc/GMT-9","Etc/GMT-10","Etc/GMT-11","Etc/GMT-12","Etc/GMT-13",
        "Etc/GMT-14","Etc/GMT+0","Etc/GMT+1","Etc/GMT+2","Etc/GMT+3","Etc/GMT+4","Etc/GMT+5","Etc/GMT+6","Etc/GMT+7","Etc/GMT+8","Etc/GMT+9",
        "Etc/GMT+10","Etc/GMT+11","Etc/GMT+12","Etc/GMT0","Etc/Greenwich","Etc/UCT","Etc/Universal","Etc/UTC","Etc/Zulu","Europe/Astrakhan",
        "Europe/Istanbul","Europe/Kaliningrad","Europe/Kirov","Europe/Minsk","Europe/Moscow","Europe/Samara","Europe/Saratov","Europe/Simferopol",
        "Europe/Ulyanovsk","Europe/Volgograd","GMT","GMT-0","GMT+0","GMT0","Greenwich","Hongkong","HST","Iceland","Indian/Antananarivo",
        "Indian/Chagos","Indian/Christmas","Indian/Cocos","Indian/Comoro","Indian/Kerguelen","Indian/Mahe","Indian/Maldives","Indian/Mauritius",
        "Indian/Mayotte","Indian/Reunion","Jamaica","Japan","Kwajalein","Libya","MST","Pacific/Apia","Pacific/Bougainville","Pacific/Chuuk",
        "Pacific/Efate","Pacific/Enderbury","Pacific/Fakaofo","Pacific/Fiji","Pacific/Funafuti","Pacific/Galapagos","Pacific/Gambier",
        "Pacific/Guadalcanal","Pacific/Guam","Pacific/Honolulu","Pacific/Johnston","Pacific/Kiritimati","Pacific/Kosrae",
        "Pacific/Kwajalein","Pacific/Majuro","Pacific/Marquesas","Pacific/Midway","Pacific/Nauru","Pacific/Niue","Pacific/Noumea","Pacific/Pago_Pago",
        "Pacific/Palau","Pacific/Pitcairn","Pacific/Pohnpei","Pacific/Ponape","Pacific/Port_Moresby","Pacific/Rarotonga","Pacific/Saipan",
        "Pacific/Samoa","Pacific/Tahiti","Pacific/Tarawa","Pacific/Tongatapu","Pacific/Truk","Pacific/Wake","Pacific/Wallis","Pacific/Yap","PRC",
        "ROC","ROK","Singapore","Turkey","UCT","Universal","US/Arizona","US/Hawaii","US/Samoa","UTC","W-SU"
        };
    for (const auto& timezone : timezones) {
        cctz::time_zone ctz;
        CHECK(cctz::load_time_zone(timezone, &ctz));
        _s_cached_timezone.emplace(timezone, std::move(ctz));
    }
    auto civil = cctz::civil_second(2021, 12, 1, 8, 30, 1);
    for (const auto& timezone1 : timezones) {
        for (const auto& timezone2 : timezones) {
            const auto tp1 = cctz::convert(civil, _s_cached_timezone[timezone1]);
            const auto tp2 = cctz::convert(civil, _s_cached_timezone[timezone2]);
            std::pair<std::string_view, std::string_view> key = {timezone1, timezone2};
            _s_cached_offsets[key] = tp1.time_since_epoch().count() - tp2.time_since_epoch().count();
        }
    }

    // other cached timezone
    // only cache CCTZ, won't caculate offsets
    static std::vector<std::string> other_timezones = {
        "Africa/Casablanca","Africa/Ceuta","Africa/El_Aaiun","America/Adak","America/Anchorage","America/Asuncion","America/Atka",
        "America/Bahia_Banderas","America/Boise","America/Cambridge_Bay","America/Chicago","America/Chihuahua","America/Denver",
        "America/Detroit","America/Edmonton","America/Ensenada","America/Fort_Wayne","America/Glace_Bay","America/Godthab",
        "America/Goose_Bay","America/Grand_Turk","America/Halifax","America/Havana","America/Indiana/Indianapolis","America/Indiana/Knox",
        "America/Indiana/Marengo","America/Indiana/Petersburg","America/Indiana/Tell_City","America/Indiana/Vevay","America/Indiana/Vincennes",
        "America/Indiana/Winamac","America/Indianapolis","America/Inuvik","America/Iqaluit","America/Juneau","America/Kentucky/Louisville",
        "America/Kentucky/Monticello","America/Knox_IN","America/Los_Angeles","America/Louisville","America/Matamoros","America/Mazatlan",
        "America/Menominee","America/Merida","America/Metlakatla","America/Mexico_City","America/Miquelon","America/Moncton","America/Monterrey",
        "America/Montreal","America/Nassau","America/New_York","America/Nipigon","America/Nome","America/North_Dakota/Beulah",
        "America/North_Dakota/Center","America/North_Dakota/New_Salem","America/Ojinaga","America/Pangnirtung",
        "America/Port-au-Prince","America/Rainy_River","America/Rankin_Inlet","America/Resolute","America/Santa_Isabel","America/Santiago",
        "America/Scoresbysund","America/Shiprock","America/Sitka","America/St_Johns","America/Thule","America/Thunder_Bay","America/Tijuana",
        "America/Toronto","America/Vancouver","America/Winnipeg","America/Yakutat","America/Yellowknife","Antarctica/Macquarie","Antarctica/McMurdo",
        "Antarctica/South_Pole","Antarctica/Troll","Arctic/Longyearbyen","Asia/Amman","Asia/Beirut","Asia/Damascus","Asia/Famagusta","Asia/Gaza",
        "Asia/Hebron","Asia/Jerusalem","Asia/Nicosia","Asia/Tehran","Asia/Tel_Aviv","Atlantic/Azores","Atlantic/Bermuda","Atlantic/Canary",
        "Atlantic/Faeroe","Atlantic/Faroe","Atlantic/Jan_Mayen","Atlantic/Madeira","Australia/ACT","Australia/Adelaide","Australia/Broken_Hill",
        "Australia/Canberra","Australia/Currie","Australia/Hobart","Australia/LHI","Australia/Lord_Howe","Australia/Melbourne","Australia/NSW",
        "Australia/South","Australia/Sydney","Australia/Tasmania","Australia/Victoria","Australia/Yancowinna","Canada/Atlantic","Canada/Central",
        "Canada/Eastern","Canada/Mountain","Canada/Newfoundland","Canada/Pacific","CET","Chile/Continental","Chile/EasterIsland","CST6CDT","Cuba",
        "EET","Eire","EST5EDT","Europe/Amsterdam","Europe/Andorra","Europe/Athens","Europe/Belfast","Europe/Belgrade","Europe/Berlin","Europe/Bratislava",
        "Europe/Brussels","Europe/Bucharest","Europe/Budapest","Europe/Busingen","Europe/Chisinau","Europe/Copenhagen","Europe/Dublin","Europe/Gibraltar",
        "Europe/Guernsey","Europe/Helsinki","Europe/Isle_of_Man","Europe/Jersey","Europe/Kiev","Europe/Lisbon","Europe/Ljubljana","Europe/London",
        "Europe/Luxembourg","Europe/Madrid","Europe/Malta","Europe/Mariehamn","Europe/Monaco","Europe/Nicosia","Europe/Oslo","Europe/Paris",
        "Europe/Podgorica","Europe/Prague","Europe/Riga","Europe/Rome","Europe/San_Marino","Europe/Sarajevo","Europe/Skopje","Europe/Sofia",
        "Europe/Stockholm","Europe/Tallinn","Europe/Tirane","Europe/Tiraspol","Europe/Uzhgorod","Europe/Vaduz","Europe/Vatican","Europe/Vienna",
        "Europe/Vilnius","Europe/Warsaw","Europe/Zagreb","Europe/Zaporozhye","Europe/Zurich","GB","GB-Eire","Iran","Israel","MET","Mexico/BajaNorte",
        "Mexico/BajaSur","Mexico/General","MST7MDT","Navajo","NZ","NZ-CHAT","Pacific/Auckland","Pacific/Chatham","Pacific/Easter","Pacific/Norfolk",
        "Poland","Portugal","PST8PDT","US/Alaska","US/Aleutian","US/Central","US/East-Indiana","US/Eastern","US/Indiana-Starke","US/Michigan","US/Mountain",
        "US/Pacific","WET"
        };
    for (const auto& timezone : other_timezones) {
        cctz::time_zone ctz;
        CHECK(cctz::load_time_zone(timezone, &ctz));
        _s_cached_timezone.emplace(timezone, std::move(ctz));
    }
}

bool TimezoneUtils::find_cctz_time_zone(const std::string_view& timezone, cctz::time_zone& ctz) {
    re2::StringPiece value;
    if (auto iter = _s_cached_timezone.find(timezone); iter != _s_cached_timezone.end()) {
        ctz = iter->second;
        return true;
    } else if (time_zone_offset_format_reg.Match(re2::StringPiece(timezone.data(), timezone.size()), 0, timezone.size(),
                                                 RE2::UNANCHORED, &value, 1)) {
        bool positive = (value[0] != '-');

        //Regular expression guarantees hour and minute mush be int
        int hour = std::stoi(value.substr(1, 2).as_string());
        int minute = std::stoi(value.substr(4, 2).as_string());

        // timezone offsets around the world extended from -12:00 to +14:00
        if (!positive && hour > 12) {
            return false;
        } else if (positive && hour > 14) {
            return false;
        }
        int offset = hour * 60 * 60 + minute * 60;
        offset *= positive ? 1 : -1;
        ctz = cctz::fixed_time_zone(cctz::seconds(offset));
        return true;
    } else if (timezone == "CST") {
        // Supports offset and region timezone type, "CST" use here is compatibility purposes.
        ctz = cctz::fixed_time_zone(cctz::seconds(8 * 60 * 60));
        return true;
    } else {
        return cctz::load_time_zone(std::string(timezone), &ctz);
    }
}

bool TimezoneUtils::timezone_offsets(const std::string_view& src, const std::string_view& dst, int64_t* offset) {
    if (const auto iter = _s_cached_offsets.find(std::make_pair(src, dst)); iter != _s_cached_offsets.end()) {
        *offset = iter->second;
        return true;
    }
    return false;
}

bool TimezoneUtils::find_cctz_time_zone(const TimezoneHsScan& timezone_hsscan, const std::string_view& timezone,
                                        cctz::time_zone& ctz) {
    // find time_zone by cache
    if (auto iter = _s_cached_timezone.find(timezone); iter != _s_cached_timezone.end()) {
        ctz = iter->second;
        return true;
    }

    bool v = false;
    hs_scan(
            timezone_hsscan.database, timezone.data(), timezone.size(), 0, timezone_hsscan.scratch,
            [](unsigned int id, unsigned long long from, unsigned long long to, unsigned int flags, void* ctx) -> int {
                *((bool*)ctx) = true;
                return 1;
            },
            &v);

    if (v) {
        bool positive = (timezone.substr(0, 1) != "-");

        //Regular expression guarantees hour and minute mush be int
        int hour = 0;
        std::string_view hour_str = timezone.substr(1, 2);
        std::from_chars(hour_str.begin(), hour_str.end(), hour);
        int minute = 0;
        std::string_view minute_str = timezone.substr(4, 5);
        std::from_chars(minute_str.begin(), minute_str.end(), minute);

        // timezone offsets around the world extended from -12:00 to +14:00
        if (!positive && hour > 12) {
            return false;
        } else if (positive && hour > 14) {
            return false;
        }
        int offset = hour * 60 * 60 + minute * 60;
        offset *= positive ? 1 : -1;
        ctz = cctz::fixed_time_zone(cctz::seconds(offset));
        return true;
    }

    if (timezone == "CST") {
        // Supports offset and region timezone type, "CST" use here is compatibility purposes.
        ctz = cctz::fixed_time_zone(cctz::seconds(8 * 60 * 60));
        return true;
    } else {
        return cctz::load_time_zone(std::string(timezone), &ctz);
    }
}

int64_t TimezoneUtils::to_utc_offset(const cctz::time_zone& ctz) {
    cctz::time_zone utc = cctz::utc_time_zone();
    const std::chrono::time_point<std::chrono::system_clock> tp;
    const cctz::time_zone::absolute_lookup a = ctz.lookup(tp);
    const cctz::time_zone::absolute_lookup b = utc.lookup(tp);
    return a.cs - b.cs;
}

} // namespace starrocks