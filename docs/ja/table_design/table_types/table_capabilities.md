---
displayed_sidebar: docs
sidebar_position: 10
---

# 各テーブルタイプの機能

## キーカラムとソートキー

<table>
<thead>
<tr><th width= "10"> </th><th><strong>主キーテーブル</strong></th><th><strong>重複キーテーブル</strong></th><th><strong>集計テーブル</strong></th><th><strong>ユニークキーテーブル</strong></th></tr>
</thead>
<tbody><tr><td><strong>キーカラムとUNIQUE制約</strong> </td><td>主キーにはUNIQUE制約とNOT NULL制約があります。</td><td>重複キーにはUNIQUE制約がありません。</td><td>集計キーにはUNIQUE制約があります。</td><td>ユニークキーにはUNIQUE制約があります。</td></tr><tr><td><strong>キーカラムとデータ変更の論理的関係</strong> <strong></strong> </td><td>新しいデータ行の主キー値がテーブル内の既存のデータ行と同じ場合、ユニーク制約違反が発生します。その場合、新しいデータ行が既存のデータ行を置き換えます。<br />ユニークキーテーブルと比較して、主キーテーブルは強化された基礎ストレージエンジンを持ち、ユニークキーテーブルを置き換えることができます。</td><td>重複キーにはUNIQUE制約がありません。そのため、新しいデータ行の重複キー値がテーブル内の既存のデータ行と同じ場合、新旧のデータ行はテーブル内に保持されます。</td><td>新しいデータ行の集計キー値がテーブル内の既存のデータ行と同じ場合、新旧のデータ行は集計キーと値カラムの集計関数に基づいて集計されます。</td><td>新しいデータ行のユニークキー値がテーブル内の既存のデータ行と同じ場合、新しいデータ行が既存のデータ行を置き換えます。<br />ユニークキーテーブルは、集計関数がreplaceである集計テーブルと見なすことができます。</td></tr><tr><td><strong>キーカラムとソートキーの関係</strong></td><td>v3.0.0以降、ソートキーは主キーテーブルの主キーから切り離されています。</td><td colspan="3">キーカラムとソートキーは結合されています。</td></tr><tr><td><strong>キーカラムとソートキーがサポートするデータタイプ</strong>  </td><td>数値（整数とBOOLEANを含む）、文字列、日付（DATEとDATETIME）。</td><td colspan="3">数値（整数、BOOLEAN、DECIMALを含む）、文字列、日付（DATEとDATETIME）。</td></tr><tr><td><strong>キーカラムとパーティション/バケットカラムの関係</strong> </td><td>パーティションカラムとバケットカラムは主キーに含まれている必要があります。</td><td>なし</td><td>パーティションカラムとバケットカラムは集計キーに含まれている必要があります。</td><td>パーティションカラムとバケットカラムはユニークキーに含まれている必要があります。</td></tr></tbody>
</table>

## キーおよび値カラムのデータタイプ

キーカラムは以下のデータタイプをサポートします: 数値（整数、BOOLEAN、DECIMALを含む）、文字列、日付（DATEとDATETIME）。

:::note
主キーテーブルのキーカラムはDECIMALデータタイプをサポートしません。
:::

一方、値カラムは基本的なデータタイプをサポートし、数値、文字列、日付（DATEとDATETIME）を含みます。BITMAP、HLL、および半構造化タイプのサポートは、異なるテーブルタイプの値カラムによって異なります。詳細は以下の通りです:
<table>
<thead>
<tr><th> </th><th><strong>主キーテーブル</strong></th><th><strong>重複キーテーブル</strong></th><th><strong>集計テーブル</strong></th><th><strong>ユニークキーテーブル</strong></th></tr>
</thead>
<tbody><tr><td><strong>BITMAP</strong></td><td>サポート</td><td>サポートされていません</td><td>サポート。集計関数はbitmap_union、replace、またはreplace_if_not_nullでなければなりません。</td><td>サポート</td></tr><tr><td><strong>HLL</strong></td><td>サポート</td><td>サポートされていません</td><td>サポート。集計関数はhll_union、replace、またはreplace_if_not_nullでなければなりません。</td><td>サポート</td></tr><tr><td><strong>PERCENTILE</strong></td><td>サポート</td><td>サポートされていません</td><td>サポート。集計関数はpercentile_union、replace、またはreplace_if_not_nullでなければなりません。</td><td>サポート</td></tr><tr><td><strong>半構造化データタイプ：JSON/ARRAY/MAP/STRUCT</strong></td><td>サポート</td><td>サポート</td><td>サポート。集計関数はreplaceまたはreplace_if_not_nullでなければなりません。</td><td>サポート</td></tr></tbody>
</table>

## データ変更

<table>
<thead>
<tr><th> </th><th><strong>主キーテーブル</strong></th><th><strong>重複キーテーブル</strong></th><th><strong>集計テーブル</strong></th><th><strong>ユニークキーテーブル</strong></th></tr>
</thead>
<tbody><tr><td><strong>データロードによるINSERT</strong> </td><td  rowspan="2">サポート。[ロードジョブで<code>__op=0</code>を設定してINSERTを実現](../../loading/Load_to_Primary_Key_tables.md)。<br />内部実装では、StarRocksはINSERTとUPDATE操作の両方をUPSERT操作と見なします。</td><td>サポート</td><td>サポート（同じ集計キー値を持つデータ行は集計されます。）</td><td>サポート（同じユニークキー値を持つデータ行は更新されます。）</td></tr><tr><td><strong>データロードによるUPDATE</strong> </td><td>サポートされていません</td><td>サポート（集計関数としてreplaceを使用することで実現できます。）</td><td>サポート（ユニークキーテーブル自体はreplace集計関数を使用する集計テーブルと見なすことができます。）</td></tr><tr><td><strong>データロードによるDELETE</strong> </td><td>サポート。[ロードジョブで<code>__op=1</code>を設定してDELETEを実現](../../loading/Load_to_Primary_Key_tables.md)。</td><td colspan="3">サポートされていません</td></tr><tr><td><strong>ロードされるデータカラム値の整合性</strong> </td><td>デフォルトでは、すべてのカラム値をロードする必要があります。ただし、部分カラム更新（<code>partial_update</code>）が有効になっている場合や、カラムにデフォルト値がある場合は、すべてのカラム値をロードする必要はありません。</td><td>デフォルトでは、すべてのカラム値をロードする必要があります。ただし、カラムにデフォルト値がある場合は、すべてのカラム値をロードする必要はありません。</td><td>デフォルトでは、すべてのカラム値をロードする必要があります。ただし、集計テーブルは値カラムの集計関数をREPLACE_IF_NOT_NULLとして指定することで部分カラム更新を実現できます。詳細は[aggr_type](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md#column_definition)を参照してください。また、カラムにデフォルト値がある場合は、すべてのカラム値をロードする必要はありません。</td><td>デフォルトでは、すべてのカラム値をロードする必要があります。ただし、カラムにデフォルト値がある場合は、すべてのカラム値をロードする必要はありません。</td></tr><tr><td><strong>[DML INSERT](../../sql-reference/sql-statements/loading_unloading/INSERT.md)</strong></td><td colspan="4">サポート</td></tr><tr><td><strong>[DML UPDATE](../../sql-reference/sql-statements/table_bucket_part_index/UPDATE.md)</strong> </td><td><ul><li>キーカラムをフィルター条件として使用: サポート</li><li>値カラムをフィルター条件として使用: サポート</li></ul></td><td colspan="3">サポートされていません</td></tr><tr><td><strong>[DML DELETE](../../sql-reference/sql-statements/table_bucket_part_index/DELETE.md)</strong></td><td><ul><li>キーカラムをフィルター条件として使用: サポート</li><li>値カラムをフィルター条件として使用: サポート</li></ul></td><td><ul><li>キーカラムをフィルター条件として使用: サポート</li><li>値カラムをフィルター条件として使用: サポート</li></ul>ただし、=、&lt;、&gt;などのキーまたは値カラム自体に基づく単純なフィルター条件のみがサポートされます。関数やサブクエリなどの複雑なフィルター条件はサポートされていません。</td><td colspan="2"><ul><li>キーカラムをフィルター条件として使用: サポート。ただし、=、&lt;、&gt;などのキーカラム自体に基づく単純なフィルター条件のみがサポートされます。関数やサブクエリなどの複雑なフィルター条件はサポートされていません。</li><li>値カラムをフィルター条件として使用: サポートされていません。</li></ul> </td></tr></tbody>
</table>

## 他の機能との互換性

<table>
<thead>
<tr><th colspan="2"></th><th><strong>主キーテーブル</strong></th><th><strong>重複キーテーブル</strong></th><th><strong>集計テーブル</strong></th><th><strong>ユニークキーテーブル</strong></th></tr>
</thead>
<tbody><tr><td rowspan="2"><strong>ビットマップインデックス/ブルームフィルターインデックス</strong></td><td><strong>キーカラムにインデックスを構築</strong></td><td colspan="4">サポート</td></tr><tr><td><strong>値カラムにインデックスを構築</strong></td><td>サポート</td><td>サポート</td><td>サポートされていません</td><td>サポートされていません</td></tr><tr><td rowspan="2"><strong>パーティション化/バケット化</strong></td><td><strong>式に基づくパーティション化/リストパーティション化</strong></td><td colspan="4">サポート</td></tr><tr><td><strong>ランダムバケット法</strong></td><td>サポートされていません</td><td>v3.1以降でサポート</td><td>サポートされていません</td><td>サポートされていません</td></tr><tr><td  rowspan="2"><strong>マテリアライズドビュー</strong></td><td ><strong>非同期マテリアライズドビュー</strong></td><td colspan="4">サポート</td></tr><tr><td><strong>同期マテリアライズドビュー</strong></td><td>サポートされていません</td><td>サポート</td><td>サポート</td><td>サポート</td></tr><tr><td rowspan="2"><strong>その他の機能</strong></td><td><strong>CTAS</strong></td><td>サポート</td><td>サポート</td><td>サポートされていません</td><td>サポートされていません</td></tr><tr><td><strong>バックアップとリストア</strong></td><td>v2.5以降でサポート</td><td colspan="3">サポート</td></tr></tbody>
</table>