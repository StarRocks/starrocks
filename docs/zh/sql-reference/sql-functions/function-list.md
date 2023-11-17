---
displayed_sidebar: "Chinese"
---

# 函数列表

StarRocks 提供了丰富的函数，方便您在日常数据查询和分析时使用。除了常见的函数分类，StarRocks 也支持 ARRAY、JSON、MAP、STRUCT 等半结构化函数，支持 [Lambda 高阶函数](Lambda_expression.md)。如果以上函数都不符合您的需求，您还可以自行编写 [Java UDF](JAVA_UDF.md) 来满足业务需求。

您可以按照以下分类来查找目标函数。

- [函数列表](#函数列表)
  - [日期函数](#日期函数)
  - [字符串函数](#字符串函数)
  - [聚合函数](#聚合函数)
  - [数学函数](#数学函数)
  - [Array 函数](#array-函数)
  - [Bitmap 函数](#bitmap-函数)
  - [JSON 函数](#json-函数)
  - [Map 函数](#map-函数)
  - [Struct 函数](#struct-函数)
  - [表函数](#表函数)
  - [Bit 函数](#bit-函数)
  - [Binary 函数](#binary-函数)
  - [加密函数](#加密函数)
  - [模糊/正则匹配函数](#模糊正则匹配函数)
  - [条件函数](#条件函数)
  - [百分位函数](#百分位函数)
  - [标量函数](#标量函数)
  - [工具函数](#工具函数)
  - [地理位置函数](#地理位置函数)
  - [Hash 函数](#hash-函数)

## 日期函数

| 函数                |                 功能      |
|  :-:                |                :-:       |
|  [add_months](./date-time-functions/add_months.md)  |   在给定的日期（DATE、DATETIME）上增加一个整数月。     |
|  [adddate，days_add](./date-time-functions/adddate.md)          |  向日期添加指定的时间间隔。        |
|  [convert_tz](./date-time-functions/convert_tz.md)          |   将给定的时间转化为另一个时区的时间。  |
|  [current_date，curdate](./date-time-functions/curdate.md)          |   获取当前的日期，以 DATE 类型返回。  |
|  [current_time，curtime](./date-time-functions/curtime.md)      |  获取当前的时间，以 TIME 类型返回。  |
|  [current_timestamp](./date-time-functions/current_timestamp.md)      |  获取当前时间，以 DATETIME 类型返回。   |
|  [date](./date-time-functions/date.md)      |  从日期或时间日期表达式中截取日期部分。  |
|  [date_add](./date-time-functions/date_add.md)      |  向日期添加指定的时间间隔。    |
|[date_diff](./date-time-functions/date_diff.md)| 根据指定的时间单位返回两个日期的差值。 |
|  [date_format](./date-time-functions/date_format.md)      |  按照 format 指定的格式来显示日期/时间数据。   |
|  [date_slice](./date-time-functions/date_slice.md)      |  根据指定的时间粒度周期，将给定的时间转化到其所在的时间粒度周期的起始或结束时刻。  |
|  [date_sub, subdate](./date-time-functions/date_sub.md)    |    从日期中减去指定的时间间隔。   |
|   [date_trunc](./date-time-functions/date_trunc.md)     |    根据指定的精度级别，将一个日期时间截断。  |
|  [datediff](./date-time-functions/datediff.md)   |  计算两个日期的差值，结果精确到天。        |
|  [day](./date-time-functions/day.md) | 返回指定日期中的天信息。|
| [dayname](./date-time-functions/dayname.md)| 返回指定日期对应的星期名称。|
| [dayofmonth](./date-time-functions/dayofmonth.md)| 返回日期中的天信息，返回值范围 1~31。  |
| [dayofweek](./date-time-functions/dayofweek.md)| 返回指定日期的工作日索引值。  |
| [dayofyear](./date-time-functions/dayofyear.md)|  计算指定日期为对应年中的哪一天。   |
| [days_add](./date-time-functions/adddate.md)| 向日期添加指定的时间间隔。  |
| [days_diff](./date-time-functions/days_diff.md)|  计算开始时间和结束时间相差几天。 |
| [days_sub](./date-time-functions/days_sub.md)| 从给定日期或者日期时间中减去指定的天数，获得新的 DATETIME 结果。  |
| [from_days](./date-time-functions/from_days.md)|  通过计算当前时间距离 0000-01-01 的天数，计算出当前是时间哪一天。 |
| [from_unixtime](./date-time-functions/from_unixtime.md)|  将 UNIX 时间戳转化为对应的时间格式。 |
| [hour](./date-time-functions/hour.md)| 获得指定日期中的小时信息。  |
| [hours_add](./date-time-functions/hours_add.md)| 给指定的日期时间增加指定的小时数。  |
| [hours_diff](./date-time-functions/hours_diff.md)|  计算开始时间和结束时间相差多少个小时。 |
| [hours_sub](./date-time-functions/hours_sub.md)| 从指定的日期时间中减少指定的小时数。  |
|[last_day](./date-time-functions/last_day.md)| 根据指定的时间单位，返回输入的日期对应的最后一天。|
|[makedate](./date-time-functions/makedate.md)| 根据给定的年份和天数值，构造一个日期值。 |
| [microseconds_add](./date-time-functions/microseconds_add.md)| 向一个日期时间添加指定的时间间隔，单位为微秒。  |
| [microseconds_sub](./date-time-functions/microseconds_sub.md)| 从一个日期时间中减去指定的时间间隔，单位为微秒。  |
| [minute](./date-time-functions/minute.md)| 获得日期中的分钟的信息，返回值范围为 0~59。  |
| [minutes_add](./date-time-functions/minutes_add.md)| 给指定的日期时间或日期增加指定的分钟数。|
| [minutes_diff](./date-time-functions/minutes_diff.md)| 计算开始时间和结束时间相差多少分钟。  |
| [minutes_sub](./date-time-functions/minutes_sub.md)| 从指定的日期时间或日期中减去指定的分钟数。  |
| [month](./date-time-functions/month.md)|  返回指定日期中的月份。 |
| [monthname](./date-time-functions/monthname.md)|  返回指定日期对应的月份。 |
| [months_add](./date-time-functions/months_add.md)| 给日期添加指定的月数。  |
| [months_diff](./date-time-functions/months_diff.md)| 计算开始时间和结束时间相差几个月。  |
| [months_sub](./date-time-functions/months_sub.md)|  从日期中减去指定的月数。 |
|[next_day](./date-time-functions/next_day.md)|根据输入的日期值，返回它之后的那个星期几所对应的日期。 |
| [now](./date-time-functions/now.md)| 获取当前的时间，以 DATETIME 类型返回。  |
| [previous_day](./date-time-functions/previous_day.md) | 根据输入的日期值，返回它之前的那个星期几所对应的日期。 |
| [quarter](./date-time-functions/quarter.md)| 返回给定的日期值对应的季度，范围 1~4。  |
| [second](./date-time-functions/second.md)|  获得日期中的秒的信息，返回值范围 0~59。 |
| [seconds_add](./date-time-functions/seconds_add.md)| 向一个日期时间添加指定的时间间隔，单位为秒。  |
| [seconds_diff](./date-time-functions/seconds_diff.md)| 计算开始时间和结束时间相差多少秒。  |
| [seconds_sub](./date-time-functions/seconds_sub.md)|  给指定的日期时间或日期减去指定的秒数。 |
| [str_to_date](./date-time-functions/str_to_date.md)| 按照 format 指定的格式将 str 转换为 DATETIME 类型的值。  |
| [str2date](./date-time-functions/str2date.md)| 按照 format 指定的格式将 str 转换为 DATE 类型的值。  |
| [time_slice](./date-time-functions/time_slice.md)| 根据指定的时间粒度周期，将给定的时间转化为其所在的时间粒度周期的起始或结束时刻。  |
| [time_to_sec](./date-time-functions/time_to_sec.md)| 将 time 时间值转换为秒数。  |
| [timediff](./date-time-functions/timediff.md)| 返回两个 DATETIME 类型值之间的差值，返回 TIME 类型。  |
| [timestamp](./date-time-functions/timestamp.md)|  将时间表达式转换为 DATETIME 值。 |
| [timestampadd](./date-time-functions/timestampadd.md)| 将整数表达式间隔添加到日期或日期时间表达式中。  |
| [timestampdiff](./date-time-functions/timestampdiff.md)|  返回两个日期或日期时间表达式的差值。 |
| [to_days](./date-time-functions/to_days.md)| 返回指定日期距离 0000-01-01 的天数。  |
| [to_date](./date-time-functions/to_date.md)| 返回 DATETIME 类型值中的日期部分。  |
| [unix_timestamp](./date-time-functions/unix_timestamp.md)| 将 DATE 或 DATETIME 类型的值转化为 UNIX 时间戳。  |
| [utc_timestamp](./date-time-functions/utc_timestamp.md)| 返回当前 UTC 日期时间。  |
| [week](./date-time-functions/week.md)| 根据指定的周数计算逻辑，计算指定的日期时间属于一年中的第几周。  |
| [weekofyear](./date-time-functions/weekofyear.md)|  计算指定的日期时间属于一年中的第几周。 |
| [weeks_add](./date-time-functions/weeks_add.md)|  向原始的日期时间加上指定周数。 |
| [weeks_diff](./date-time-functions/weeks_diff.md)|  计算开始时间和结束时间相差几周。 |
| [weeks_sub](./date-time-functions/weeks_sub.md)| 从原始的日期中减去若干周数。  |
| [year](./date-time-functions/year.md)|  返回指定日期时间中的年份。 |
| [years_add](./date-time-functions/years_add.md)| 向原始的日期时间添加指定的年数。  |
| [years_diff](./date-time-functions/years_diff.md)|  计算开始时间和结束时间相差几年。 |
| [years_sub](./date-time-functions/years_sub.md)  |  从指定的日期时间中减去指定年数。     |

## 字符串函数

| 函数                |                 功能      |
|  :-:                |                :-:       |
| [append_trailing_char_if_absent](./string-functions/append_trailing_char_if_absent.md)   | 如果字符串非空并且末尾不包含 trailing_char 字符，则将 trailing_char 字符附加到末尾。  |
| [ascii](./string-functions/ascii.md) | 返回字符串第一个字符对应的 ASCII 码。  |
| [char](./string-functions/char.md)| 根据输入的 ASCII 值返回对应的字符。  |
| [char_length，character_length](./string-functions/char_length.md) | 返回字符串的长度。  |
| [concat](./string-functions/concat.md) |  将多个字符串连接起来。 |
| [concat_ws](./string-functions/concat_ws.md) | 使用分隔符将两个或以上的字符串拼接成一个新的字符串。  |
| [ends_with](./string-functions/ends_with.md) | 如果字符串以指定后缀结尾，返回 true，否则返回 false。  |
| [find_in_set](./string-functions/find_in_set.md) | 返回指定的字符串在一系列字符串列表中第一次出现的位置。  |
| [group_concat](./string-functions/group_concat.md) | 将结果集中的多行结果连接成一个字符串。  |
| [hex_decode_binary](./string-functions/hex_decode_binary.md) |  将一个十六进制编码的字符串解码为 VARBINARY 类型的值。 |
| [hex_decode_string](./string-functions/hex_decode_string.md) | 将输入字符串中每一对十六进制数字解析为一个数字，并将解析得到的数字转换为表示该数字的字节，然后返回一个二进制字符串。  |
| [hex](./string-functions/hex.md) | 对于输入的数字或字符，返回十六进制的字符串表示形式。  |
| [instr](./string-functions/instr.md) |  返回子字符串在指定的字符串中第一次出现的位置。 |
| [left](./string-functions/left.md) | 从字符串左边部分返回指定长度的字符。  |
| [length](./string-functions/length.md) | 返回字符串的字节长度。  |
| [locate](./string-functions/locate.md) | 从 pos 下标开始的字符串处开始查找子字符串在某个字符串中第一次出现的位置。  |
| [lower](./string-functions/lower.md) | 将参数中所有的字符串转换成小写。  |
| [lpad](./string-functions/lpad.md) |根据指定的长度在字符串前面（左侧）追加字符。  |
| [ltrim](./string-functions/ltrim.md) |  从字符串的左侧（开始部分）移除连续出现的空格或指定字符。 |
| [money_format](./string-functions/money_format.md) |  将数字按照货币格式输出，整数部分每隔 3 位用逗号分隔，小数部分保留 2 位。  |
| [null_or_empty](./string-functions/null_or_empty.md) | 如果字符串为空字符串或者 NULL 则返回 true，否则返回 false。 |
| [parse_url](./string-functions/parse_url.md) |  从目标 URL 中提取一部分信息。 |
| [repeat](./string-functions/repeat.md) | 将字符串重复 count 次输出，count 小于 1 时返回空字符串。  |
| [replace](./string-functions/replace.md) | 将字符串中符合指定模式的字符全部替换成其他字符。  |
| [reverse](./string-functions/reverse.md) | 将字符串或数组反转，返回的字符串或数组的顺序和源字符串或数组的顺序相反。  |
| [right](./string-functions/right.md) | 从字符串右边部分返回指定长度的字符。  |
| [rpad](./string-functions/rpad.md) |  根据指定的长度在字符串后面（右侧）追加字符。 |
| [rtrim](./string-functions/rtrim.md) | 从字符串的右侧（结尾部分）移除连续出现的空格或指定字符。  |
| [space](./string-functions/space.md) | 返回由指定数量的空格组成的字符串。|
| [split](./string-functions/split.md) | 根据分隔符拆分字符串，将拆分后的所有字符串以 ARRAY 的格式返回。  |
| [split_part](./string-functions/split_part.md) | 根据分割符拆分字符串，返回指定的分割部分。  |
| [starts_with](./string-functions/starts_with.md) | 如果字符串以指定前缀开头返回 1，否则返回 0。  |
| [str_to_map](./string-functions/str_to_map.md) | 将给定的字符串分割成键值对 (Key-Value pair)，返回包含这些键值对的 Map。  |
| [strleft](./string-functions/strleft.md) | 从字符串左边部分返回指定长度的字符。  |
| [strright](./string-functions/strright.md) | 从字符串右边部分返回指定长度的字符。  |
| [substr, substring](./string-functions/substring.md) | 返回字符串中从位置 pos 开始的指定长度的子字符串。  |
| [trim](./string-functions/trim.md) | 从字符串的左侧和右侧移除连续出现的空格或指定的字符。  |
| [ucase](./string-functions/ucase.md) | 该函数与 upper 一致，将字符串转换为大写形式。  |
| [unhex](./string-functions/unhex.md) | 将输入的字符串中的两个字符为一组转化为 16 进制的字符，然后拼接成字符串输出。  |
| [upper](./string-functions/upper.md) | 将字符串转换为大写形式。  |

## 聚合函数

| 函数                |                 功能      |
|  :-:                |                :-:       |
|  [any_value](./aggregate-functions/any_value.md)| 在包含 GROUP BY 的聚合查询中，该函数用于从每个聚合分组中**随机**选择一行返回。 |
|  [approx_count_distinct](./aggregate-functions/approx_count_distinct.md)| 返回类似于 COUNT(DISTINCT col) 结果的近似值。 |
|  [array_agg](./array-functions/array_agg.md) | 将一列中的值（包括空值 null）串联成一个数组 (多行转一行）。  |
|  [avg](./aggregate-functions/avg.md)| 用于返回选中字段的平均值。 |
|  [bitmap](./aggregate-functions/bitmap.md)| 通过 bitmap 函数实现聚合。 |
|  [bitmap_agg](./bitmap-functions/bitmap_agg.md)| 将一列中的多行非 NULL 数值合并成一行 BITMAP 值，即多行转一行。 |
| [corr](./aggregate-functions/corr.md) | 返回两个随机变量的皮尔逊相关系数. |
| [covar_pop](./aggregate-functions/covar_pop.md)| 返回两个随机变量的总体协方差。 |
| [covar_samp](./aggregate-functions/covar_samp.md)| 返回两个随机变量的样本协方差。 |
|  [count](./aggregate-functions/count.md)| 返回满足条件的行数。 |
|  [group_concat](./string-functions/group_concat.md)| 将结果集中的多行结果连接成一个字符串。|
|  [grouping](./aggregate-functions/grouping.md)| 判断一个列是否为聚合列，如果是聚合列则返回 0，否则返回 1。|
|  [grouping_id](./aggregate-functions/grouping_id.md)| 用于区分相同分组标准的分组统计结果。 |
|  [hll_empty](./aggregate-functions/hll_empty.md)| 生成空 HLL 列，用于 INSERT 或导入数据时补充默认值。 |
|  [hll_hash](./aggregate-functions/hll_hash.md)| 将一个数值转换为 HLL 类型。通常用于导入中，将源数据中的数值映射到 StarRocks 表中的 HLL 列类型。 |
|  [hll_raw_agg](./aggregate-functions/hll_raw_agg.md)| 用于聚合 HLL 类型的字段，返回 HLL 类型。 |
|  [hll_union](./aggregate-functions/hll_union.md)| 返回一组 HLL 值的并集。 |
|  [hll_union_agg](./aggregate-functions/hll_union_agg.md)| 将多个 HLL 类型数据合并成一个 HLL。 |
|  [max](./aggregate-functions/max.md)| 返回表达式中的最大值。 |
|  [max_by](./aggregate-functions/max_by.md)| 返回与 y 的最大值相关联的 x 值。 |
|  [min](./aggregate-functions/min.md)| 返回表达式中的最小值。 |
|  [min_by](./aggregate-functions/min_by.md) | 返回与 y 的最小值关联的 x 值。 |
|  [multi_distinct_count](./aggregate-functions/multi_distinct_count.md)| 返回表达式去除重复值后的行数，功能等同于 COUNT(DISTINCT expr)。 |
|  [multi_distinct_sum](./aggregate-functions/multi_distinct_sum.md)| 返回表达式去除重复值后的总和，功能等同于 sum(distinct expr)。 |
|  [percentile_approx](./aggregate-functions/percentile_approx.md)| 返回第 p 个百分位点的近似值。 |
|  [percentile_cont](./aggregate-functions/percentile_cont.md)| 计算精确百分位数。 |
|  [percentile_disc](./aggregate-functions/percentile_disc.md)| 计算百分位数。 |
|  [retention](./aggregate-functions/retention.md)| 用于计算一段时间内的用户留存情况。  |
|  [sum](./aggregate-functions/sum.md)| 返回指定列所有值的总和。 |
|  [std](./aggregate-functions/std.md)| 返回指定列的标准差。 |
|  [stddev，stddev_pop](./aggregate-functions/stddev.md)| 返回表达式的总体标准差。 |
|  [stddev_samp](./aggregate-functions/stddev_samp.md)| 返回表达式的样本标准差。 |
|  [variance, variance_pop, var_pop](./aggregate-functions/variance.md)| 返回表达式的方差。 |
|  [var_samp](./aggregate-functions/var_samp.md)| 返回表达式的样本方差。 |
|  [window_funnel](./aggregate-functions/window_funnel.md)| 搜索滑动时间窗口内的事件列表，计算条件匹配的事件链里的最大连续事件数。 |

## 数学函数

| 函数                |                 功能      |
|  :-:                |                :-:       |
|  [abs](./math-functions/abs.md)| 计算绝对值。 |
|  [acos](./math-functions/acos.md)| 计算反余弦值（单位为弧度）。 |
|  [asin](./math-functions/asin.md)| 计算反正弦值（单位为弧度）。 |
|  [atan](./math-functions/atan.md)| 计算反正切值（单位为弧度）。 |
|  [atan2](./math-functions/atan2.md)| 通过使用两个参数的符号确定象限，计算 x/y 的反正切的主值，返回值在 [-π, π] 范围内。 |
|  [bin](./math-functions/bin.md)| 将输入的参数转成二进制。 |
|  [ceil, dceil](./math-functions/ceil.md)| 返回大于或等于 x 的最小整数。 |
|  [ceiling](./math-functions/ceiling.md)| 返回大于或等于 x 的最小整数。 |
|  [conv](./math-functions/conv.md)| 对输入的参数进行进制转换。 |
|  [cos](./math-functions/cos.md)| 计算余弦值。 |
|  [cosh](./math-functions/cosh.md)| 计算输入数值的双曲余弦值。 |
|  [cosine_similarity](./math-functions/cos_similarity.md)| 计算两个向量的余弦夹角来评估向量之间的相似度。 |
|  [cosine_similarity_norm](./math-functions/cos_similarity_norm.md)| 计算两个归一化向量的余弦夹角来评估向量之间的相似度。|
|  [cot](./math-functions/cot.md)| 计算余切值（单位为弧度）。 |
|  [degrees](./math-functions/degrees.md)| 将参数 x 转成角度，x 是弧度。 |
|  [divide](./math-functions/divide.md)| 除法函数，返回 x 除以 y 的结果， |
|  [e](./math-functions/e.md)| 返回自然对数函数的底数。 |
|  [exp, dexp](./math-functions/exp.md)| 返回 e 的 x 次幂。 |
|  [floor, dfloor](./math-functions/floor.md)| 返回不大于 x 的最大整数值。 |
|  [fmod](./math-functions/fmod.md)| 取模函数，返回两个数相除之后的浮点余数。 |
|  [greatest](./math-functions/greatest.md)| 返回多个输入参数中的最大值。 |
|  [least](./math-functions/least.md)| 返回多个输入参数中的最小值。 |
|  [ln, dlog1, log](./math-functions/ln.md)| 返回参数 x 的自然对数，以 e 为底数。 |
|  [log](./math-functions/log.md)| 返回以 base 为底数的 x 的对数。如果未指定 base，则该函数等同于 ln()。 |
|  [log2](./math-functions/log2.md)| 返回以 2 为底数的 x 的对数。 |
|  [log10, dlog10](./math-functions/log10.md)| 返回以 10 为底数的 x 的对数。 |
|  [mod](./math-functions/mod.md)| 取模函数，返回两个数相除之后的余数。 |
|  [multiply](./math-functions/multiply.md)| 计算两个参数的乘积。 |
|  [negative](./math-functions/negative.md)| 返回参数的负数。 |
|  [pi](./math-functions/pi.md)| 返回圆周率。 |
|  [pmod](./math-functions/pmod.md)| 取模函数，返回两个数相除之后的正余数。 |
|  [positive](./math-functions/positive.md)| 返回表达式的结果。 |
|  [pow, power, dpow, fpow](./math-functions/pow.md)| 返回参数 x 的 y 次方。 |
|  [radians](./math-functions/radians.md)| 将参数 x 转为弧度，x 是角度。 |
|  [rand, random](./math-functions/rand.md)| 返回一个 0 (包含) 到 1（不包含）之间的随机浮点数。 |
|  [round, dround](./math-functions/round.md)| 按照指定的小数位数对数值进行四舍五入。 |
|  [sign](./math-functions/sign.md)| 返回参数 x 的符号。 |
|  [sin](./math-functions/sin.md)| 计算参数 x 的正弦，x 为弧度值。 |
|  [sinh](./math-functions/sinh.md)| 计算输入数值的双曲正弦值。 |
|  [sqrt, dsqrt](./math-functions/sqrt.md)| 计算参数的平方根。 |
|  [square](./math-functions/square.md)| 计算参数的平方。 |
|  [tan](./math-functions/tan.md)| 计算参数 x 的正切，x 为弧度值。 |
|  [tanh](./math-functions/tanh.md)| 计算输入数值的双曲正切值。 |
|  [truncate](./math-functions/truncate.md)| 返回数值 x 保留到小数点后 y 位的值。 |

## Array 函数

| 函数                |                 功能      |
|  :-:                |                :-:       |
|  [all_match](./array-functions/all_match.md)| 判断数组中的所有元素是否都匹配谓词中指定的条件。 |
|  [any_match](./array-functions/any_match.md)| 判断数组中是否有元素匹配谓词中指定的条件。 |
|  [array_agg](./array-functions/array_agg.md)| 将一列中的值（包括空值 null）串联成一个数组 (多行转一行）。 |
|  [array_append](./array-functions/array_append.md)| 在数组末尾添加一个新的元素。 |
|  [array_avg](./array-functions/array_avg.md)| 求取一个ARRAY中的所有数据的平均数。 |
|  [array_concat](./array-functions/array_concat.md)| 将多个数组拼接成一个数组。 |
|  [array_contains](./array-functions/array_contains.md)| 检查数组中是否包含某个元素，是的话返回 1，否则返回 0。 |
|  [array_contains_all](./array-functions/array_contains_all.md)| 检查数组 arr1 是否包含数组 arr2 中的所有元素。 |
|  [array_cum_sum](./array-functions/array_cum_sum.md)| 对数组中的元素进行向前累加。 |
|  [array_difference](./array-functions/array_difference.md)| 对于数值型数组，返回相邻两个元素的差(从后者中减去前者)构成的数组。 |
|  [array_distinct](./array-functions/array_distinct.md)| 数组元素去重。 |
|  [array_filter](./array-functions/array_filter.md)| 根据设定的过滤条件返回数组中匹配的元素。 |
|  [array_generate](./array-functions/array_generate.md)| 生成一个包含数值元素的数组，数值范围在 start 和 end 之间，步长为 step。 |
|  [array_intersect](./array-functions/array_intersect.md)| 对于多个同类型数组，返回交集。 |
|  [array_join](./array-functions/array_join.md)| 将数组中的所有元素连接生成一个字符串。 |
|  [array_length](./array-functions/array_length.md)| 计算数组中的元素个数。 |
|  [array_map](./array-functions/array_map.md)| 用于将输入的 arr1，arr2 等数组按照 lambda_function 进行转换，输出一个新的数组。 |
|  [array_max](./array-functions/array_max.md)| 求取一个ARRAY中的所有数据中的最大值。 |
|  [array_min](./array-functions/array_min.md)| 求取一个ARRAY中的所有数据中的最小值。 |
|  [arrays_overlap](./array-functions/arrays_overlap.md)| 判断两个相同类型的数组中是否包含相同的元素。 |
|  [array_position](./array-functions/array_position.md)| 获取数组中某个元素位置，是的话返回位置，否则返回 0. |
|  [array_remove](./array-functions/array_remove.md)| 从数组中移除指定元素。 |
|  [array_slice](./array-functions/array_slice.md)| 返回数组的一个数组片段。 |
|  [array_sort](./array-functions/array_sort.md)| 对数组中的元素进行升序排列。 |
|  [array_sortby](./array-functions/array_sortby.md)| 对数组中的元素根据另外一个键值数组元素或者 Lambda 函数生成的键值数组元素进行升序排列。 |
|  [array_sum](./array-functions/array_sum.md)| 对数组中的所有元素求和。 |
|  [array_to_bitmap](./array-functions/array_to_bitmap.md)| 将 array 类型转化为 bitmap 类型。 |
|  [cardinality](./array-functions/cardinality.md)| 计算数组中的元素个数， |
|  [element_at](./array-functions/element_at.md)| 获取 Array 数组中指定位置的元素。 |
|  [reverse](./string-functions/reverse.md)| 将字符串或数组反转，返回的字符串或数组的顺序和源字符串或数组的顺序相反。 |
|  [unnest](./array-functions/unnest.md)| 表函数，用于将一个数组展开成多行。 |

## Bitmap 函数

| 函数                |                 功能      |
|  :-:                |                :-:       |
|  [bitmap_agg](./bitmap-functions/bitmap_agg.md)| 将一列中的多行非 NULL 数值合并成一行 BITMAP 值，即多行转一行。 |
|  [bitmap_and](./bitmap-functions/bitmap_and.md)| 计算两个 bitmap 的交集，返回新的 bitmap。|
|  [bitmap_andnot](./bitmap-functions/bitmap_andnot.md)| 计算两个输入的 bitmap 的差集。|
|  [bitmap_contains](./bitmap-functions/bitmap_contains.md)| 计算输入值是否在 Bitmap 列中。 |
|  [bitmap_count](./bitmap-functions/bitmap_count.md)| 统计 bitmap 中不重复值的个数。 |
|  [bitmap_empty](./bitmap-functions/bitmap_empty.md)| 返回一个空 bitmap，主要用于 insert 或 stream load 时填充默认值。|
|  [bitmap_from_string](./bitmap-functions/bitmap_from_string.md)| 将一个字符串转化为一个 bitmap，字符串由逗号分隔的一组 UInt32 数字组成。|
|  [bitmap_hash](./bitmap-functions/bitmap_hash.md)| 对任意类型的输入计算 32 位的哈希值，返回包含该哈希值的 bitmap。|
|  [bitmap_has_any](./bitmap-functions/bitmap_has_any.md)| 计算两个 Bitmap 列是否存在相交元素。|
|  [bitmap_intersect](./bitmap-functions/bitmap_intersect.md)| 求一组 bitmap 值的交集。|
|  [bitmap_max](./bitmap-functions/bitmap_max.md)| 获取 Bitmap 中的最大值。|
|  [bitmap_min](./bitmap-functions/bitmap_min.md)| 获取 Bitmap 中的最小值。|
|  [bitmap_or](./bitmap-functions/bitmap_or.md)| 计算两个 bitmap 的并集，返回新的 bitmap。|
|  [bitmap_remove](./bitmap-functions/bitmap_remove.md)| 从 Bitmap 中删除指定的数值。 |
| [bitmap_subset_in_range](./bitmap-functions/bitmap_subset_in_range.md)| 从 Bitmap 中返回取值在指定范围内的元素。|
| [bitmap_subset_limit](./bitmap-functions/bitmap_subset_limit.md)| 根据指定的起始值，从 BITMAP 中截取指定个数的元素。|
|  [bitmap_to_array](./bitmap-functions/bitmap_to_array.md)| 将 BITMAP 中的所有值组合成 BIGINT 类型的数组。|
|  [bitmap_to_base64](./bitmap-functions/bitmap_to_base64.md)| 将 bitmap 转换为 Base64 字符串。|
|  [base64_to_bitmap](./bitmap-functions/base64_to_bitmap.md)|将 Base64 编码的字符串转化为 Bitmap。 |
|  [bitmap_to_string](./bitmap-functions/bitmap_to_string.md)| 将一个 bitmap 转化成一个逗号分隔的字符串。|
|  [bitmap_union](./bitmap-functions/bitmap_union.md)| 求一组 bitmap 值的并集。 |
|  [bitmap_union_count](./bitmap-functions/bitmap_union_count.md)| 计算一组 bitmap 值的并集，并返回并集的基数。|
|  [bitmap_union_int](./bitmap-functions/bitmap_union_int.md)| 计算 TINYINT，SMALLINT 和 INT 类型的列中不重复值的个数。|
|  [bitmap_xor](./bitmap-functions/bitmap_xor.md)| 计算两个 Bitmap 中不重复元素所构成的集合。|
|  [intersect_count](./bitmap-functions/intersect_count.md)| 求 bitmap 交集大小。|
|  [sub_bitmap](./bitmap-functions/sub_bitmap.md)| 计算两个 bitmap 之间相同元素的个数。|
|  [to_bitmap](./bitmap-functions/to_bitmap.md)| 将输入值转换为 bitmap。 |

## JSON 函数

| 函数                |                 功能      |
|  :-:                |                :-:       |
|  [json_array](./json-functions/json-constructor-functions/json_array.md)| 接收 SQL 数组并返回一个 JSON 类型的数组。|
|  [json_object](./json-functions/json-constructor-functions/json_object.md)| 接收键值集合，返回一个包含这些键值对的 JSON 类型的对象。|
|  [parse_json](./json-functions/json-constructor-functions/parse_json.md)|将字符串类型的数据构造为 JSON 类型的数据。|
|  [箭头函数](./json-functions/json-query-and-processing-functions/arrow-function.md)| 箭头函数可以查询 JSON 对象中指定路径的值。|
|  [cast](./json-functions/json-query-and-processing-functions/cast.md)| 实现 JSON 类型数据与 SQL 类型间的相互转换。|
|  [get_json_double](./json-functions/json-query-and-processing-functions/get_json_double.md)| 解析并获取 JSON 字符串内指定路径中的浮点型内容。|
|  [get_json_int](./json-functions/json-query-and-processing-functions/get_json_int.md)| 解析并获取 JSON 字符串内指定路径中的整型内容。|
|  [get_json_string](./json-functions/json-query-and-processing-functions/get_json_string.md)| 解析并获取 JSON 字符串内指定路径中的字符串。|
|  [json_each](./json-functions/json-query-and-processing-functions/json_each.md)| 将 JSON 对象的最外层按照键和值展开为两列，返回一行或多行数据的集合。|
|  [json_exists](./json-functions/json-query-and-processing-functions/json_exists.md)| 查询 JSON 对象中指定路径是否存在满足特定条件的值。|
|  [json_keys](./json-functions/json-query-and-processing-functions/json_keys.md)| 返回 JSON 对象中所有最上层成员 (key) 组成的数组。|
|  [json_length](./json-functions/json-query-and-processing-functions/json_length.md)| 计算 JSON 字符串的长度。|
|  [json_query](./json-functions/json-query-and-processing-functions/json_query.md)| 查询 JSON 对象中指定路径（json_path）的值，并输出 JSON 类型的结果。 |
|  [json_string](./json-functions/json-query-and-processing-functions/json_string.md)| 将 JSON 类型转化为 JSON 字符串。|
|  [to_json](./json-functions/json-query-and-processing-functions/to_json.md)| 将 Map 或 Struct 类型的数据转换成 JSON 数据。 |

## Map 函数

| 函数                |                 功能      |
|  :-:                |                :-:       |
|  [cardinality](./map-functions/cardinality.md)| 计算 Map 中元素的个数。 |
|  [distinct_map_keys](./map-functions/distinct_map_keys.md)| 删除 Map 中重复的 Key。 |
|  [element_at](./map-functions/element_at.md)| 获取 Map 中指定键 (Key) 对应的值 (Value)。 |
|  [map_apply](./map-functions/map_apply.md)| 返回 Map 中所有 Key 或 Value 进行 Lambda 函数运算后的 Map 值。 |
|  [map_concat](./map-functions/map_concat.md)| 将多个 Map 合并成一个 Map。 |
|  [map_filter](./map-functions/map_filter.md)| 根据设定的过滤函数返回 MAP 中匹配的 Key-value 对。 |
|  [map_from_arrays](./map-functions/map_from_arrays.md)| 将两个 ARRAY 数组作为 Key 和 Value 组合成一个 MAP 对象。 |
|  [map_keys](./map-functions/map_keys.md)| 返回 Map 中所有 key 组成的数组。 |
|  [map_size](./map-functions/map_size.md)| 计算 Map 中元素的个数。 |
|  [map_values](./map-functions/map_values.md)| 返回 Map 中所有 Value 组成的数组。 |
|  [transform_keys](./map-functions/transform_keys.md)| 对 Map 中的 key 进行 Lambda 转换。 |
|  [transform_values](./map-functions/transform_values.md)| 对 Map 中的 value 进行 lambda 转换。 |

## Struct 函数

| 函数                |                 功能      |
|  :-:                |                :-:       |
|  [named_struct](./struct-functions/named_struct.md)| 根据给定的字段名和字段值来构建 STRUCT。 |
|  [row](./struct-functions/row.md)| 根据给定的一个或多个值来构建 STRUCT。 |

## 表函数

| 函数                |                 功能      |
|  :-:                |                :-:       |
| [files](./table-functions/files.md) | 从云存储或 HDFS 读取数据文件。|
| [generate_series](./table-functions/generate_series.md) | 生成一系列从 start 到 end 的数值，步长为 step。 |
| [json_each](./json-functions/json-query-and-processing-functions/json_each.md) | 将 JSON 对象的最外层按照键和值展开为两列，返回一行或多行数据的集合。 |
| [unnest](./array-functions/unnest.md) | 用于将一个数组展开成多行。|

## Bit 函数

| 函数                |                 功能      |
|  :-:                |                :-:       |
|  [bitand](./bit-functions/bitand.md)| 返回两个数值在按位进行 AND 运算后的结果。 |
|  [bitnot](./bit-functions/bitnot.md)| 返回参数 `x` 进行取反运算后的结果。 |
|  [bitor](./bit-functions/bitor.md)| 返回两个数值在按位进行 OR 运算后的结果。  |
|  [bitxor](./bit-functions/bitxor.md)| 返回两个数值在按位 XOR 运算后的结果。 |
|  [bit_shift_left](./bit-functions/bit_shift_left.md)| 将一个数值或数值表达式的二进制表示向左移动指定的位数。该函数执行算术左移。 |
|  [bit_shift_right](./bit-functions/bit_shift_right.md)| 将一个数值或者数值表达式的二进制表示向右移动指定的位数。该函数执行算术右移。 |
|  [bit_shift_right_logical](./bit-functions/bit_shift_right_logical.md)| 将一个数值或者数值表达式的二进制表示向右移动指定的位数。该函数执行逻辑右移。 |

## Binary 函数

| 函数                |                 功能      |
|  :-:                |                :-:       |
|  [from_binary](./binary-functions/from_binary.md)| 根据指定的格式，将二进制数据转化为 VARCHAR 类型的字符串。 |
|  [to_binary](./binary-functions/to_binary.md)| 根据指定的二进制格式 (binary_type)，将 VARCHAR 字符串转换为二进制类型。|

## 加密函数

| 函数                |                 功能      |
|  :-:                |                :-:       |
|  [aes_decrypt](./crytographic-functions/aes_decrypt.md)| 使用 AES_128_ECB 算法将字符串解密并返回一个二进制字符串。 |
|  [aes_encrypt](./crytographic-functions/aes_encrypt.md)| 使用 AES_128_ECB 算法对字符串进行加密并返回一个二进制字符串。 |
|  [base64_decode_binary](./crytographic-functions/base64_decode_binary.md)| 解码某个 Base64 编码的字符串，并返回一个 VARBINARY 类型的值。 |
|  [base64_decode_string](./crytographic-functions/base64_decode_string.md)| 用于解码某个 Base64 编码的字符串，是 to_base64() 函数的反向函数。 |
|  [from_base64](./crytographic-functions/from_base64.md)| 将 Base64 编码过的字符串 str 进行解码。反向函数为 to_base64。 |
|  [md5](./crytographic-functions/md5.md)| 使用 MD5 加密算法将给定字符串进行加密，输出一个 128-bit 的校验和 (checksum)，以 32 字符的十六进制字符串表示。 |
|  [md5sum](./crytographic-functions/md5sum.md)| 计算多个输入参数的 MD5 128-bit 校验和 (checksum)，以 32 字符的十六进制字符串表示。 |
|  [sha2](./crytographic-functions/sha2.md)| 计算 SHA-2 系列哈希函数 (SHA-224/SHA-256/SHA-384/SHA-512)。 |
|  [sm3](./crytographic-functions/sm3.md)| 使用 SM3 摘要算法，将字符串加密为 256-bit 的 十六进制字符串，每 32 位用空格分隔。 |
|  [to_base64](./crytographic-functions/to_base64.md)| 将字符串 str 进行 Base64 编码。反向函数为 from_base64。 |

## 模糊/正则匹配函数

| 函数                |                 功能      |
|  :-:                |                :-:       |
|  [like](./like_predicate-functions/like.md) | 判断字符串是否**模糊匹配**给定的模式 `pattern`。 |
|  [regexp](./like_predicate-functions/regexp.md) | 判断字符串是否匹配给定的正则表达式 `pattern`。 |
|  [regexp_extract](./like_predicate-functions/regexp_extract.md) | 对字符串进行正则匹配，抽取符合 pattern 的第 pos 个匹配部分，需要 pattern 完全匹配 str 中的某部分，才能返回 pattern 部分中需匹配部分，如果没有匹配就返回空字符串。 |
|  [regexp_replace](./like_predicate-functions/regexp_replace.md) | 对字符串进行正则匹配，将命中 pattern 的部分使用 repl 来进行替换。 |

## 条件函数

| 函数                |                 功能      |
|  :-:                |                :-:       |
|  [case](./condition-functions/case_when.md)| 将表达式与一个值比较。如果能找到匹配项，则返回 THEN 中的结果。如果未找到匹配项，则返回 ELSE 中的结果。 |
|  [coalesce](./condition-functions/coalesce.md)| 从左向右返回参数中的第一个非 NULL 表达式。 |
|  [if](./condition-functions/if.md)| 若参数 expr1 成立，返回结果 expr2；否则返回结果 expr3。 |
|  [ifnull](./condition-functions/ifnull.md)| 若 expr1 不为 NULL，返回 expr1。若 expr1 为 NULL，返回 expr2。 |
|  [nullif](./condition-functions/nullif.md)| 若参数 expr1 与 expr2 相等，则返回 NULL，否则返回 expr1 的值。 |

## 百分位函数

| 函数                |                 功能      |
|  :-:                |                :-:       |
|  [percentile_approx_raw](./percentile-functions/percentile_approx_raw.md)| 计算给定参数 x 的百分位数。 |
|  [percentile_empty](./percentile-functions/percentile_empty.md)| 构造一个 percentile 类型的数值，主要用于 INSERT 或 Stream Load 导入时填充默认值。 |
|  [percentile_hash](./percentile-functions/percentile_hash.md)| 将 double 类型数值构造成 percentile 类型数值。 |
|  [percentile_union](./percentile-functions/percentile_union.md)| 用于对分组结果进行聚合。 |

## 标量函数

| 函数                |                 功能      |
|  :-:                |                :-:       |
| [hll_cardinality](./scalar-functions/hll_cardinality.md) |  用于计算 HLL 类型值的基数。  |

## 工具函数

| 函数                |                 功能      |
|  :-:                |                :-:       |
| [catalog](./utility-functions/catalog.md)| 查询当前会话所在的 Catalog。 |
|  [current_role](./utility-functions/current_role.md)| 获取当前用户激活的角色。  |
|  [current_version](./utility-functions/current_version.md)| 获取当前 StarRocks 的版本 |
| [database](./utility-functions/database.md)| 查询当前会话所在的数据库。 |
|  [host_name](./utility-functions/host_name.md)| 获取计算所在节点的主机名。|
|  [isnull](./utility-functions/isnull.md)| 判断输入值是否为 NULL。|
| [is_role_in_session](./utility-functions/is_role_in_session.md) | 检查指定的角色（包括嵌套角色）在当前会话下是否已经激活。 |
|  [last_query_id](./utility-functions/last_query_id.md)| 返回最近一次执行的查询的 ID。|
|  [sleep](./utility-functions/sleep.md)| 将当前正在执行的线程休眠 x 秒。|
|  [uuid](./utility-functions/uuid.md)| 以 VARCHAR 形式返回一个随机的 UUID 值。长度为36个字符，包含32个十六进制字符，由4个连字符进行连接，形式为8-4-4-4-12。|
|  [uuid_numeric](./utility-functions/uuid_numeric.md)|返回一个数值类型的随机 UUID 值。 |
|  [version](./utility-functions/version.md)|返回当前 MySQL 数据库的版本。  |

## 地理位置函数

| 函数                |                 功能      |
|  :-:                |                :-:       |
|  [ST_AsText, ST_AsWKT](./spatial-functions/st_astext.md)| 将一个几何图形转化为 WKT（Well Known Text）的表示形式。 |
|  [st_circle](./spatial-functions/st_circle.md)| 将一个 WKT (Well Known Text) 转化为地球球面上的一个圆。 |
|  [st_contains](./spatial-functions/st_contains.md)| 判断几何图形 shape1 是否完全能够包含几何图形 shape2。 |
|  [st_distance_sphere](./spatial-functions/st_distance_sphere.md)| 计算地球两点之间的球面距离，单位是米。 |
|  [st_geometryfromtext](./spatial-functions/st_geometryfromtext.md)| 将一个 WKT（Well Known Text）转化为对应的内存的几何形式。 |
|  [st_linefromtext, ST_LineStringFromText](./spatial-functions/st_linefromtext.md)| 将一个 WKT（Well Known Text）转化为一个 Line 形式的内存表现形式。 |
|  [st_point](./spatial-functions/st_point.md)| 通过给定的 X 坐标值、Y 坐标值返回对应的 Point。 |
|  [st_polygon](./spatial-functions/st_polygon.md)| 将一个 WKT（Well Known Text）转化为对应的多边形内存形式。 |
|  [st_x](./spatial-functions/st_x.md)| 当 point 是一个合法的 POINT 类型时，返回对应的 X 坐标值。 |
|  [st_y](./spatial-functions/st_y.md)| 当 point 是一个合法的 POINT 类型时，返回对应的 Y 坐标值。 |

## Hash 函数

| 函数                |                 功能      |
|  :-:                |                :-:       |
| [murmur_hash3_32](./hash-functions/murmur_hash3_32.md) | 返回输入字符串的 32 位 murmur3 hash 值。 |
