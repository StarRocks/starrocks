---
displayed_sidebar: docs
---

# タイムゾーンの設定

このトピックでは、タイムゾーンの設定方法とその影響について説明します。

## セッションレベルまたはグローバルタイムゾーンの設定

`time_zone` パラメータを使用して、StarRocks クラスターのセッションレベルまたはグローバルタイムゾーンを設定できます。

- セッションレベルのタイムゾーンを設定するには、`SET time_zone = 'xxx';` コマンドを実行します。異なるセッションに対して異なるタイムゾーンを設定できます。FEs との接続が切れると、タイムゾーンの設定は無効になります。
- グローバルタイムゾーンを設定するには、`SET global time_zone = 'xxx';` コマンドを実行します。タイムゾーンの設定は FEs に保存され、FEs との接続が切れても有効です。

> **Note**
>
> StarRocks にデータをロードする前に、StarRocks クラスターのグローバルタイムゾーンを `system_time_zone` パラメータと同じ値に変更してください。そうしないと、データロード後に DATE 型のデータが正しくなくなります。`system_time_zone` パラメータは、FEs をホストするマシンのタイムゾーンを指します。マシンが起動されると、マシンのタイムゾーンがこのパラメータの値として記録されます。このパラメータを手動で設定することはできません。

### タイムゾーンの形式

`time_zone` パラメータの値は大文字と小文字を区別しません。このパラメータの値は、次の形式のいずれかで指定できます。

| **Format**     | **Example**                                                  |
| -------------- | ------------------------------------------------------------ |
| UTC offset     | `SET time_zone = '+10:00';` `SET global time_zone = '-6:00';` |
| Time zone name | `SET time_zone = 'Asia/Shanghai';` `SET global time_zone = 'America/Los_Angeles';` |

タイムゾーンの形式についての詳細は、[List of tz database time zones](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones) を参照してください。

> **Note**
>
> CST を除いて、タイムゾーンの略語はサポートされていません。`time_zone` の値を `CST` に設定すると、StarRocks は `CST` を `Asia/Shanghai` に変換します。

### デフォルトのタイムゾーン

`time_zone` パラメータのデフォルト値は `Asia/Shanghai` です。

## タイムゾーン設定の確認

タイムゾーンの設定を確認するには、次のコマンドを実行します。

```plaintext
 SHOW VARIABLES LIKE '%time_zone%';
```

## タイムゾーン設定の影響

- タイムゾーンの設定は、SHOW LOAD および SHOW BACKENDS ステートメントによって返される時間値に影響します。ただし、CREATE TABLE ステートメントで指定されたパーティション列が DATE または DATETIME 型の場合、`LESS THAN` 句で指定された値には影響しません。また、DATE および DATETIME 型のデータにも影響しません。
- タイムゾーンの設定は、次の関数の表示と保存に影響します。
  - **from_unixtime**: 指定された UTC タイムスタンプに基づいて、指定したタイムゾーンの日付と時間を返します。たとえば、StarRocks クラスターのグローバルタイムゾーンが `Asia/Shanghai` の場合、`select FROM_UNIXTIME(0);` は `1970-01-01 08:00:00` を返します。
  - **unix_timestamp**: 指定したタイムゾーンの日付と時間に基づいて UTC タイムスタンプを返します。たとえば、StarRocks クラスターのグローバルタイムゾーンが `Asia/Shanghai` の場合、`select UNIX_TIMESTAMP('1970-01-01 08:00:00');` は `0` を返します。
  - **curtime**: 指定したタイムゾーンの現在の時間を返します。たとえば、指定したタイムゾーンの現在の時間が 16:34:05 の場合、`select CURTIME();` は `16:34:05` を返します。
  - **now**: 指定したタイムゾーンの現在の日付と時間を返します。たとえば、指定したタイムゾーンの現在の日付と時間が 2021-02-11 16:34:13 の場合、`select NOW();` は `2021-02-11 16:34:13` を返します。
  - **convert_tz**: 日付と時間をあるタイムゾーンから別のタイムゾーンに変換します。たとえば、`select CONVERT_TZ('2021-08-01 11:11:11', 'Asia/Shanghai', 'America/Los_Angeles');` は `2021-07-31 20:11:11` を返します。