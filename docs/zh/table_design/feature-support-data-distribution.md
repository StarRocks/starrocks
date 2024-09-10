---
displayed_sidebar: docs
sidebar_label: "能力边界"
---

# 功能边界：数据分布

本文档概述了 StarRocks 支持的分区和分桶功能。

## 支持的表类型

- **分桶**

  所有表类型均支持哈希分桶。随机分桶从 v3.1 版本开始支持，**仅支持明细表**。

- **分区**

  从 v3.1 版本开始，表达式分区、Range 分区和 List 分区均支持所有表类型。

## 分桶

<table>
    <tr>
        <th>功能</th>
        <th>关键功能点</th>
        <th>支持状态</th>
        <th>备注</th>
    </tr>
    <tr>
        <td rowspan="2">分桶策略</td>
        <td>哈希分桶</td>
        <td>是</td>
        <td></td>
    </tr>
    <tr>
        <td>随机分桶</td>
        <td>是 (v3.1+)</td>
        <td>随机分桶<strong>仅支持明细表</strong>。从 v3.2 版本开始，StarRocks 支持根据集群信息和数据大小动态调整创建的 Tablet 数量。</td>
    </tr>
    <tr>
        <td>分桶键数据类型</td>
        <td>日期、整数、字符串</td>
        <td>是</td>
        <td></td>
    </tr>
    <tr>
        <td rowspan="2">分桶数量</td>
        <td>自动设置分桶数量</td>
        <td>是 (v3.0+)</td>
        <td>自动根据 BE 节点数量或最大历史分区的数据量确定。<br />后续版本对分区表和非分区表分别进行了优化。</td>
    </tr>
    <tr>
        <td>随机分桶按需动态增加分桶数量</td>
        <td>是 (v3.2+)</td>
        <td></td>
    </tr>
</table>

## 分区

<table>
    <tr>
        <th>功能</th>
        <th>关键功能点</th>
        <th>支持状态</th>
        <th>备注</th>
    </tr>
    <tr>
        <td rowspan="3">分区策略</td>
        <td>表达式分区</td>
        <td>是 (v3.1+)</td>
        <td>
            <ul>
                <li>包括基于时间函数表达式的分区（自 v3.0 起）和基于列表达式的分区（自 v3.1 起）</li>
                <li>支持的时间函数：date_trunc、time_slice</li>
            </ul>
        </td>
    </tr>
    <tr>
        <td>Range 分区</td>
        <td>是 (v3.2+)</td>
        <td>从 v3.3.0 起，三种特定时间函数可用于分区键：from_unixtime、from_unixtime_ms、str2date、substr/substring。</td>
    </tr>
    <tr>
        <td>List 分区</td>
        <td>是 (v3.1+)</td>
        <td></td>
    </tr>
    <tr>
        <td rowspan="2">分区键数据类型</td>
        <td>日期、整数、布尔值</td>
        <td>是</td>
        <td></td>
    </tr>
    <tr>
        <td>字符串</td>
        <td>是</td>
        <td>
            <ul>
                <li>仅表达式分区和 List 分区支持字符串类型的分区键。</li>
                <li>Range 分区不支持字符串类型的分区键。需要使用 str2date 将列转换为日期类型。</li>
            </ul>
        </td>
    </tr>
</table>

### 分区策略的区别

<table>
    <tr>
        <th rowspan="2"></th>
        <th colspan="2">表达式分区</th>
        <th rowspan="2">Range 分区</th>
        <th rowspan="2">List 分区</th>
    </tr>
    <tr>
        <th>基于时间函数表达式的分区</th>
        <th>基于列表达式的分区</th>
    </tr>
    <tr>
        <td>数据类型</td>
        <td>日期 (DATE/DATETIME)</td>
        <td>
                  <ul>
                    <li>字符串 (除 BINARY)</li>
                    <li>日期 (DATE/DATETIME)</li>
                    <li>整数和布尔值</li>
           </ul>
        </td>
        <td>
                  <ul>
                    <li>字符串 (除 BINARY) [1]</li>
                    <li>日期或 Timestamp [1]</li>
                    <li>整数</li>
           </ul>
        </td>
        <td>
                  <ul>
                    <li>字符串 (除 BINARY)</li>
                    <li>日期 (DATE/DATETIME)</li>
                    <li>整数和布尔值</li>
           </ul>
        </td>
    </tr>
    <tr>
        <td>支持多个分区键</td>
        <td>/（仅支持一个日期类型的分区键）</td>
        <td>是</td>
        <td>是</td>
        <td>是</td>
    </tr>
    <tr>
        <td>支持分区键的 Null 值</td>
        <td>是</td>
        <td>/ [2]</td>
        <td>是</td>
        <td>/ [2]</td>
    </tr>
    <tr>
        <td>在数据导入前手动创建分区</td>
        <td>/ [3]</td>
        <td>/ [3]</td>
        <td>
            <ul>
                <li>如果分区是批量手动创建的，则需要手动创建</li>
                <li>如果采用动态分区策略，则无需手动创建</li>
            </ul>
        </td>
        <td>是</td>
    </tr>
    <tr>
        <td>在数据导入时自动创建分区</td>
        <td>是</td>
        <td>是</td>
        <td>/</td>
        <td>/</td>
    </tr>
</table>

:::note

- [1]\: 需要使用 from_unixtime、str2date 或其他时间函数将列转换为日期类型。

- [2]\: 从 v3.3.3 版本起，List 分区的分区键将支持 Null 值。

- [3]\: 分区将自动创建。

:::

有关 List 分区和表达式分区的详细比较，请参阅 [List 分区和表达式分区的区别](./list_partitioning.md)。

