---
displayed_sidebar: docs
sidebar_position: 50
sidebar_label: Feature Support
---

# 機能サポート: データ分散

このドキュメントは、StarRocks がサポートするパーティション化とバケット化の機能について説明します。

## サポートされているテーブルタイプ

- **バケット化**

  ハッシュバケット法はすべてのテーブルタイプでサポートされています。ランダムバケット法（v3.1以降）は**重複キーテーブルのみ**でサポートされています。

- **パーティション化**

  式に基づくパーティション化（v3.1以降）、レンジパーティション化、およびリストパーティション化（v3.1以降）はすべてのテーブルタイプでサポートされています。

## バケット化

<table>
    <tr>
        <th>機能</th>
        <th>重要ポイント</th>
        <th>サポート状況</th>
        <th>備考</th>
    </tr>
    <tr>
        <td rowspan="2">バケット化戦略</td>
        <td>ハッシュバケット法</td>
        <td>はい</td>
        <td></td>
    </tr>
    <tr>
        <td>ランダムバケット法</td>
        <td>はい (v3.1+)</td>
        <td>ランダムバケット法は<strong>重複キーテーブルのみ</strong>でサポートされています。<br />v3.2から、StarRocks はクラスタ情報とデータサイズに応じて作成するタブレットの数を動的に調整することをサポートしています。</td>
    </tr>
    <tr>
        <td>バケットキーのデータ型</td>
        <td>Date, Integer, String</td>
        <td>はい</td>
        <td></td>
    </tr>
    <tr>
        <td rowspan="2">バケット数</td>
        <td>バケット数を自動設定</td>
        <td>はい (v3.0+)</td>
        <td>BE ノードの数または最大の履歴パーティションのデータ量によって自動的に決定されます。<br />後のバージョンでは、パーティション化されたテーブルと非パーティション化されたテーブルに対して個別に最適化されています。</td>
    </tr>
    <tr>
        <td>ランダムバケット法のバケット数の動的増加</td>
        <td>はい (v3.2+)</td>
        <td></td>
    </tr>
</table>

## パーティション化

<table>
    <tr>
        <th>機能</th>
        <th>重要ポイント</th>
        <th>サポート状況</th>
        <th>備考</th>
    </tr>
    <tr>
        <td rowspan="3">パーティション化戦略</td>
        <td>式に基づくパーティション化</td>
        <td>はい (v3.1+)</td>
        <td>
            <ul>
                <li>時間関数式に基づくパーティション化（v3.0以降）およびカラム式に基づくパーティション化（v3.1以降）を含む</li>
                <li>サポートされている時間関数: date_trunc, time_slice</li>
            </ul>
        </td>
    </tr>
    <tr>
        <td>レンジパーティション化</td>
        <td>はい (v3.2+)</td>
        <td>v3.3.0以降、3つの特定の時間関数がパーティションキーに使用できます: from_unixtime, from_unixtime_ms, str2date, substr/substring。</td>
    </tr>
    <tr>
        <td>リストパーティション化</td>
        <td>はい (v3.1+)</td>
        <td></td>
    </tr>
    <tr>
        <td rowspan="2">パーティションキーのデータ型</td>
        <td>Date, Integer, Boolean</td>
        <td>はい</td>
        <td></td>
    </tr>
    <tr>
        <td>String</td>
        <td>はい</td>
        <td>
            <ul>
                <li>式に基づくパーティション化とリストパーティション化のみが String 型のパーティションキーをサポートします。</li>
                <li>レンジパーティション化は String 型のパーティションキーをサポートしません。カラムを日付型に変換するために str2date を使用する必要があります。</li>
            </ul>
        </td>
    </tr>
</table>

### パーティション化戦略の違い

<table>
    <tr>
        <th rowspan="2"></th>
        <th colspan="2">式に基づくパーティション化</th>
        <th rowspan="2">レンジパーティション化</th>
        <th rowspan="2">リストパーティション化</th>
    </tr>
    <tr>
        <th>時間関数式に基づくパーティション化</th>
        <th>カラム式に基づくパーティション化</th>
    </tr>
    <tr>
        <td>データ型</td>
        <td>Date (DATE/DATETIME)</td>
        <td>
                  <ul>
                    <li>String (BINARY を除く)</li>
                    <li>Date (DATE/DATETIME)</li>
                    <li>Integer および Boolean</li>
           </ul>
        </td>
        <td>
                  <ul>
                    <li>String (BINARY を除く) [1]</li>
                    <li>Date または timestamp [1]</li>
                    <li>Integer</li>
           </ul>
        </td>
        <td>
                  <ul>
                    <li>String (BINARY を除く)</li>
                    <li>Date (DATE/DATETIME)</li>
                    <li>Integer および Boolean</li>
           </ul>
        </td>
    </tr>
    <tr>
        <td>複数のパーティションキーのサポート</td>
        <td>/ (日付型のパーティションキーのみをサポート)</td>
        <td>はい</td>
        <td>はい</td>
        <td>はい</td>
    </tr>
    <tr>
        <td>パーティションキーの Null 値のサポート</td>
        <td>はい</td>
        <td>/ [2]</td>
        <td>はい</td>
        <td>/ [2]</td>
    </tr>
    <tr>
        <td>データロード前のパーティションの手動作成</td>
        <td>/ [3]</td>
        <td>/ [3]</td>
        <td>
            <ul>
                <li>パーティションがバッチで手動作成される場合ははい</li>
                <li>動的パーティション化戦略が採用されている場合はいいえ</li>
            </ul>
        </td>
        <td>はい</td>
    </tr>
    <tr>
        <td>データロード中のパーティションの自動作成</td>
        <td>はい</td>
        <td>はい</td>
        <td>/</td>
        <td>/</td>
    </tr>
</table>

:::note

- [1]\: カラムを日付型に変換するために from_unixtime, str2date または他の時間関数を使用する必要があります。
- [2]\: Null 値は v3.3.3 以降、リストパーティション化のパーティションキーでサポートされます。
- [3]\: パーティションは自動的に作成されます。

:::

リストパーティション化と式に基づくパーティション化の詳細な比較については、[リストパーティション化と式に基づくパーティション化の比較](list_partitioning.md)を参照してください。