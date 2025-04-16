---
displayed_sidebar: docs
sidebar_position: 0.9
---

# Java UDF

バージョン v2.2.0 以降、Java プログラミング言語を使用して、特定のビジネスニーズに合わせたユーザー定義関数 (UDF) をコンパイルできます。

バージョン v3.0 以降、StarRocks はグローバル UDF をサポートしており、関連する SQL ステートメント (CREATE/SHOW/DROP) に `GLOBAL` キーワードを含めるだけで済みます。

このトピックでは、さまざまな UDF の開発と使用方法について説明します。

現在、StarRocks はスカラー UDF、ユーザー定義集計関数 (UDAF)、ユーザー定義ウィンドウ関数 (UDWF)、およびユーザー定義テーブル関数 (UDTF) をサポートしています。

## 前提条件

- [Apache Maven](https://maven.apache.org/download.cgi) をインストールしており、Java プロジェクトを作成およびコンパイルできます。

- サーバーに JDK 1.8 をインストールしています。

- Java UDF 機能が有効になっています。この機能を有効にするには、FE 設定ファイル **fe/conf/fe.conf** の FE 設定項目 `enable_udf` を `true` に設定し、FE ノードを再起動して設定を有効にします。詳細については、[パラメーター設定](../../administration/management/FE_configuration.md)を参照してください。

## UDF の開発と使用

Maven プロジェクトを作成し、Java プログラミング言語を使用して必要な UDF をコンパイルする必要があります。

### ステップ 1: Maven プロジェクトを作成

Maven プロジェクトを作成し、基本的なディレクトリ構造は次のようになります。

```Plain
project
|--pom.xml
|--src
|  |--main
|  |  |--java
|  |  |--resources
|  |--test
|--target
```

### ステップ 2: 依存関係を追加

**pom.xml** ファイルに次の依存関係を追加します。

```XML
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>org.example</groupId>
    <artifactId>udf</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.source>8</maven.compiler.source>
        <maven.compiler.target>8</maven.compiler.target>
    </properties>

    <dependencies>
        <dependency>
            <groupId>com.alibaba</groupId>
            <artifactId>fastjson</artifactId>
            <version>1.2.76</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-dependency-plugin</artifactId>
                <version>2.10</version>
                <executions>
                    <execution>
                        <id>copy-dependencies</id>
                        <phase>package</phase>
                        <goals>
                            <goal>copy-dependencies</goal>
                        </goals>
                        <configuration>
                            <outputDirectory>${project.build.directory}/lib</outputDirectory>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-assembly-plugin</artifactId>
                <version>3.3.0</version>
                <executions>
                    <execution>
                        <id>make-assembly</id>
                        <phase>package</phase>
                        <goals>
                            <goal>single</goal>
                        </goals>
                    </execution>
                </executions>
                <configuration>
                    <descriptorRefs>
                        <descriptorRef>jar-with-dependencies</descriptorRef>
                    </descriptorRefs>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

### ステップ 3: UDF をコンパイル

Java プログラミング言語を使用して UDF をコンパイルします。

#### スカラー UDF をコンパイル

スカラー UDF は単一のデータ行を処理し、単一の値を返します。クエリでスカラー UDF を使用する場合、各行は結果セット内の単一の値に対応します。典型的なスカラ関数には `UPPER`、`LOWER`、`ROUND`、`ABS` などがあります。

JSON データ内のフィールドの値が JSON オブジェクトではなく JSON 文字列であると仮定します。SQL ステートメントを使用して JSON 文字列を抽出する場合、`GET_JSON_STRING` を 2 回実行する必要があります。例えば、`GET_JSON_STRING(GET_JSON_STRING('{"key":"{\\"k0\\":\\"v0\\"}"}', "$.key"), "$.k0")` のようにします。

SQL ステートメントを簡素化するために、JSON 文字列を直接抽出できるスカラー UDF をコンパイルできます。例えば、`MY_UDF_JSON_GET('{"key":"{\\"k0\\":\\"v0\\"}"}', "$.key.k0")` のようにします。

```Java
package com.starrocks.udf.sample;

import com.alibaba.fastjson.JSONPath;

public class UDFJsonGet {
    public final String evaluate(String obj, String key) {
        if (obj == null || key == null) return null;
        try {
            // JSONPath ライブラリは、フィールドの値が JSON 文字列であっても完全に展開できます。
            return JSONPath.read(obj, key).toString();
        } catch (Exception e) {
            return null;
        }
    }
}
```

ユーザー定義クラスは、次の表に記載されたメソッドを実装する必要があります。

> **NOTE**
>
> メソッド内のリクエストパラメーターと戻りパラメーターのデータ型は、[ステップ 6](#step-6-create-the-udf-in-starrocks) で実行される `CREATE FUNCTION` ステートメントで宣言されたものと同じであり、このトピックの「[SQL データ型と Java データ型のマッピング](#mapping-between-sql-data-types-and-java-data-types)」セクションで提供されるマッピングに準拠している必要があります。

| メソッド                     | 説明                                                  |
| -------------------------- | ------------------------------------------------------------ |
| TYPE1 evaluate(TYPE2, ...) | UDF を実行します。evaluate() メソッドは、パブリックメンバーアクセスレベルを必要とします。 |

#### UDAF をコンパイル

UDAF は複数のデータ行を処理し、単一の値を返します。典型的な集計関数には `SUM`、`COUNT`、`MAX`、`MIN` などがあり、各 GROUP BY 句で指定された複数のデータ行を集計し、単一の値を返します。

`MY_SUM_INT` という名前の UDAF をコンパイルしたいと仮定します。組み込みの集計関数 `SUM` とは異なり、`MY_SUM_INT` 関数は INT データ型のリクエストパラメーターと戻りパラメーターのみをサポートします。

```Java
package com.starrocks.udf.sample;

public class SumInt {
    public static class State {
        int counter = 0;
        public int serializeLength() { return 4; }
    }

    public State create() {
        return new State();
    }

    public void destroy(State state) {
    }

    public final void update(State state, Integer val) {
        if (val != null) {
            state.counter+= val;
        }
    }

    public void serialize(State state, java.nio.ByteBuffer buff) {
        buff.putInt(state.counter);
    }

    public void merge(State state, java.nio.ByteBuffer buffer) {
        int val = buffer.getInt();
        state.counter += val;
    }

    public Integer finalize(State state) {
        return state.counter;
    }
}
```

ユーザー定義クラスは、次の表に記載されたメソッドを実装する必要があります。

> **NOTE**
>
> メソッド内のリクエストパラメーターと戻りパラメーターのデータ型は、[ステップ 6](#step-6-create-the-udf-in-starrocks) で実行される `CREATE FUNCTION` ステートメントで宣言されたものと同じであり、このトピックの「[SQL データ型と Java データ型のマッピング](#mapping-between-sql-data-types-and-java-data-types)」セクションで提供されるマッピングに準拠している必要があります。

| メソッド                            | 説明                                                  |
| --------------------------------- | ------------------------------------------------------------ |
| State create()                    | 状態を作成します。                                             |
| void destroy(State)               | 状態を破棄します。                                            |
| void update(State, ...)           | 状態を更新します。最初のパラメーター `State` に加えて、UDF 宣言で 1 つ以上のリクエストパラメーターを指定できます。 |
| void serialize(State, ByteBuffer) | 状態をバイトバッファにシリアライズします。                     |
| void merge(State, ByteBuffer)     | 状態をバイトバッファからデシリアライズし、バイトバッファを最初のパラメーターとして状態にマージします。 |
| TYPE finalize(State)              | 状態から UDF の最終結果を取得します。            |

コンパイル中には、次の表に記載されたバッファクラス `java.nio.ByteBuffer` とローカル変数 `serializeLength` も使用する必要があります。

| クラスとローカル変数 | 説明                                                  |
| ------------------------ | ------------------------------------------------------------ |
| java.nio.ByteBuffer()    | バッファクラスで、中間結果を格納します。中間結果は、実行のためにノード間で送信される際にシリアライズまたはデシリアライズされる可能性があります。したがって、`serializeLength` 変数を使用して、中間結果のデシリアライズに許可される長さを指定する必要があります。 |
| serializeLength()        | 中間結果のデシリアライズに許可される長さ。単位: バイト。このローカル変数を INT 型の値に設定します。例えば、`State { int counter = 0; public int serializeLength() { return 4; }}` は、中間結果が INT データ型であり、デシリアライズの長さが 4 バイトであることを指定します。これらの設定は、ビジネス要件に基づいて調整できます。例えば、中間結果のデータ型を LONG に指定し、デシリアライズの長さを 8 バイトにする場合は、`State { long counter = 0; public int serializeLength() { return 8; }}` を渡します。 |

`java.nio.ByteBuffer` クラスに格納された中間結果のデシリアライズに関して、次の点に注意してください。

- `ByteBuffer` クラスに依存する remaining() メソッドを呼び出して状態をデシリアライズすることはできません。
- `ByteBuffer` クラスに対して clear() メソッドを呼び出すことはできません。
- `serializeLength` の値は、書き込まれたデータの長さと同じでなければなりません。そうでない場合、シリアライズとデシリアライズ中に不正な結果が生成されます。

#### UDWF をコンパイル

通常の集計関数とは異なり、UDWF は複数の行のセット（ウィンドウと呼ばれる）を操作し、各行に対して値を返します。典型的なウィンドウ関数には、行を複数のセットに分割する `OVER` 句が含まれています。各セットの行に対して計算を行い、各行に対して値を返します。

`MY_WINDOW_SUM_INT` という名前の UDWF をコンパイルしたいと仮定します。組み込みの集計関数 `SUM` とは異なり、`MY_WINDOW_SUM_INT` 関数は INT データ型のリクエストパラメーターと戻りパラメーターのみをサポートします。

```Java
package com.starrocks.udf.sample;

public class WindowSumInt {    
    public static class State {
        int counter = 0;
        public int serializeLength() { return 4; }
        @Override
        public String toString() {
            return "State{" +
                    "counter=" + counter +
                    '}';
        }
    }

    public State create() {
        return new State();
    }

    public void destroy(State state) {

    }

    public void update(State state, Integer val) {
        if (val != null) {
            state.counter+=val;
        }
    }

    public void serialize(State state, java.nio.ByteBuffer buff) {
        buff.putInt(state.counter);
    }

    public void merge(State state, java.nio.ByteBuffer buffer) {
        int val = buffer.getInt();
        state.counter += val;
    }

    public Integer finalize(State state) {
        return state.counter;
    }

    public void reset(State state) {
        state.counter = 0;
    }

    public void windowUpdate(State state,
                            int peer_group_start, int peer_group_end,
                            int frame_start, int frame_end,
                            Integer[] inputs) {
        for (int i = (int)frame_start; i < (int)frame_end; ++i) {
            state.counter += inputs[i];
        }
    }
}
```

ユーザー定義クラスは、UDAF に必要なメソッド（UDWF は特別な集計関数であるため）と、次の表に記載された windowUpdate() メソッドを実装する必要があります。

> **NOTE**
>
> メソッド内のリクエストパラメーターと戻りパラメーターのデータ型は、[ステップ 6](#step-6-create-the-udf-in-starrocks) で実行される `CREATE FUNCTION` ステートメントで宣言されたものと同じであり、このトピックの「[SQL データ型と Java データ型のマッピング](#mapping-between-sql-data-types-and-java-data-types)」セクションで提供されるマッピングに準拠している必要があります。

| メソッド                                                   | 説明                                                  |
| -------------------------------------------------------- | ------------------------------------------------------------ |
| void windowUpdate(State state, int, int, int , int, ...) | ウィンドウのデータを更新します。UDWF の詳細については、[ウィンドウ関数](../sql-functions/Window_function.md)を参照してください。入力として行を入力するたびに、このメソッドはウィンドウ情報を取得し、中間結果を適宜更新します。<ul><li>`peer_group_start`: 現在のパーティションの開始位置。`PARTITION BY` は、OVER 句でパーティション列を指定するために使用されます。パーティション列の値が同じ行は、同じパーティションに属すると見なされます。</li><li>`peer_group_end`: 現在のパーティションの終了位置。</li><li>`frame_start`: 現在のウィンドウフレームの開始位置。ウィンドウフレーム句は、計算範囲を指定し、現在の行と現在の行に対して指定された距離内の行をカバーします。例えば、`ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING` は、現在の行、現在の行の前の行、および現在の行の後の行をカバーする計算範囲を指定します。</li><li>`frame_end`: 現在のウィンドウフレームの終了位置。</li><li>`inputs`: ウィンドウへの入力として入力されるデータ。データは特定のデータ型のみをサポートする配列パッケージです。この例では、INT 値が入力として入力され、配列パッケージは Integer[] です。</li></ul> |

#### UDTF をコンパイル

UDTF は 1 行のデータを読み取り、複数の値を返します。これらの値はテーブルと見なされます。テーブル値関数は通常、行を列に変換するために使用されます。

> **NOTE**
>
> StarRocks は、UDTF が複数の行と 1 列からなるテーブルを返すことを許可します。

`MY_UDF_SPLIT` という名前の UDTF をコンパイルしたいと仮定します。`MY_UDF_SPLIT` 関数は、スペースを区切り文字として使用し、STRING データ型のリクエストパラメーターと戻りパラメーターをサポートします。

```Java
package com.starrocks.udf.sample;

public class UDFSplit{
    public String[] process(String in) {
        if (in == null) return null;
        return in.split(" ");
    }
}
```

ユーザー定義クラスで定義されたメソッドは、次の要件を満たす必要があります。

> **NOTE**
>
> メソッド内のリクエストパラメーターと戻りパラメーターのデータ型は、[ステップ 6](#step-6-create-the-udf-in-starrocks) で実行される `CREATE FUNCTION` ステートメントで宣言されたものと同じであり、このトピックの「[SQL データ型と Java データ型のマッピング](#mapping-between-sql-data-types-and-java-data-types)」セクションで提供されるマッピングに準拠している必要があります。

| メソッド           | 説明                         |
| ---------------- | ----------------------------------- |
| TYPE[] process() | UDTF を実行し、配列を返します。 |

### ステップ 4: Java プロジェクトをパッケージ化

次のコマンドを実行して Java プロジェクトをパッケージ化します。

```Bash
mvn package
```

**target** フォルダーに次の JAR ファイルが生成されます: **udf-1.0-SNAPSHOT.jar** と **udf-1.0-SNAPSHOT-jar-with-dependencies.jar**。

### ステップ 5: Java プロジェクトをアップロード

JAR ファイル **udf-1.0-SNAPSHOT-jar-with-dependencies.jar** を、StarRocks クラスター内のすべての FEs および BEs からアクセス可能な HTTP サーバーにアップロードします。次に、次のコマンドを実行してファイルをデプロイします。

```Bash
mvn deploy 
```

Python を使用して簡単な HTTP サーバーをセットアップし、その HTTP サーバーに JAR ファイルをアップロードすることができます。

> **NOTE**
>
> [ステップ 6](#step-6-create-the-udf-in-starrocks) では、FEs が UDF のコードを含む JAR ファイルをチェックし、チェックサムを計算し、BEs が JAR ファイルをダウンロードして実行します。

### ステップ 6: StarRocks で UDF を作成

StarRocks では、UDF をデータベース名前空間とグローバル名前空間の 2 種類の名前空間で作成できます。

- UDF に対して可視性や分離の要件がない場合、グローバル UDF として作成できます。その場合、関数名にカタログ名やデータベース名をプレフィックスとして含めずにグローバル UDF を参照できます。
- UDF に可視性や分離の要件がある場合、または異なるデータベースで同じ UDF を作成する必要がある場合、各個別のデータベースで作成できます。このようにして、セッションがターゲットデータベースに接続されている場合、関数名を使用して UDF を参照できます。セッションがターゲットデータベース以外のカタログやデータベースに接続されている場合、カタログ名とデータベース名を関数名のプレフィックスとして含めて UDF を参照する必要があります。例えば、`catalog.database.function` のようにします。

> **NOTICE**
>
> グローバル UDF を作成して使用する前に、システム管理者に連絡して必要な権限を付与してもらう必要があります。詳細については、[GRANT](../sql-statements/account-management/GRANT.md) を参照してください。

JAR パッケージをアップロードした後、StarRocks で UDF を作成できます。グローバル UDF の場合、作成ステートメントに `GLOBAL` キーワードを含める必要があります。

#### 構文

```sql
CREATE [GLOBAL][AGGREGATE | TABLE] FUNCTION function_name
(arg_type [, ...])
RETURNS return_type
PROPERTIES ("key" = "value" [, ...])
```

#### パラメーター

| **パラメーター**      | **必須** | **説明**                                                     |
| ------------- | -------- | ------------------------------------------------------------ |
| GLOBAL        | No       | グローバル UDF を作成するかどうか。v3.0 からサポートされています。 |
| AGGREGATE     | No       | UDAF または UDWF を作成するかどうか。       |
| TABLE         | No       | UDTF を作成するかどうか。`AGGREGATE` と `TABLE` の両方が指定されていない場合、スカラ関数が作成されます。               |
| function_name | Yes       | 作成したい関数の名前。このパラメーターにはデータベース名を含めることができます。例えば、`db1.my_func` のようにします。`function_name` にデータベース名が含まれている場合、UDF はそのデータベースに作成されます。そうでない場合、UDF は現在のデータベースに作成されます。新しい関数の名前とそのパラメーターは、宛先データベース内の既存の名前と同じであってはなりません。そうでない場合、関数は作成できません。関数名が同じでも、パラメーターが異なる場合は作成が成功します。 |
| arg_type      | Yes       | 関数の引数の型。追加された引数は `, ...` で表すことができます。サポートされているデータ型については、[SQL データ型と Java データ型のマッピング](#mapping-between-sql-data-types-and-java-data-types) を参照してください。|
| return_type      | Yes       | 関数の戻り型。サポートされているデータ型については、[Java UDF](#mapping-between-sql-data-types-and-java-data-types) を参照してください。 |
| PROPERTIES    | Yes       | 作成する UDF の種類に応じて異なる関数のプロパティ。 |

#### スカラ UDF を作成

次のコマンドを実行して、前の例でコンパイルしたスカラ UDF を作成します。

```SQL
CREATE [GLOBAL] FUNCTION MY_UDF_JSON_GET(string, string) 
RETURNS string
PROPERTIES (
    "symbol" = "com.starrocks.udf.sample.UDFJsonGet", 
    "type" = "StarrocksJar",
    "file" = "http://http_host:http_port/udf-1.0-SNAPSHOT-jar-with-dependencies.jar"
);
```

| パラメーター | 説明                                                  |
| --------- | ------------------------------------------------------------ |
| symbol    | UDF が属する Maven プロジェクトのクラス名。このパラメーターの値は `<package_name>.<class_name>` 形式です。 |
| type      | UDF の種類。値を `StarrocksJar` に設定します。これは、UDF が Java ベースの関数であることを指定します。 |
| file      | UDF のコードを含む JAR ファイルをダウンロードできる HTTP URL。このパラメーターの値は `http://<http_server_ip>:<http_server_port>/<jar_package_name>` 形式です。 |
| isolation | (オプション) UDF 実行間で関数インスタンスを共有し、静的変数をサポートするために、これを "shared" に設定します。 |

#### UDAF を作成

次のコマンドを実行して、前の例でコンパイルした UDAF を作成します。

```SQL
CREATE [GLOBAL] AGGREGATE FUNCTION MY_SUM_INT(INT) 
RETURNS INT
PROPERTIES 
( 
    "symbol" = "com.starrocks.udf.sample.SumInt", 
    "type" = "StarrocksJar",
    "file" = "http://http_host:http_port/udf-1.0-SNAPSHOT-jar-with-dependencies.jar"
);
```

PROPERTIES 内のパラメーターの説明は、[スカラ UDF を作成](#create-a-scalar-udf) の説明と同じです。

#### UDWF を作成

次のコマンドを実行して、前の例でコンパイルした UDWF を作成します。

```SQL
CREATE [GLOBAL] AGGREGATE FUNCTION MY_WINDOW_SUM_INT(Int)
RETURNS Int
properties 
(
    "analytic" = "true",
    "symbol" = "com.starrocks.udf.sample.WindowSumInt", 
    "type" = "StarrocksJar", 
    "file" = "http://http_host:http_port/udf-1.0-SNAPSHOT-jar-with-dependencies.jar"    
);
```

`analytic`: UDF がウィンドウ関数であるかどうか。値を `true` に設定します。他のプロパティの説明は、[スカラ UDF を作成](#create-a-scalar-udf) の説明と同じです。

#### UDTF を作成

次のコマンドを実行して、前の例でコンパイルした UDTF を作成します。

```SQL
CREATE [GLOBAL] TABLE FUNCTION MY_UDF_SPLIT(string)
RETURNS string
properties 
(
    "symbol" = "com.starrocks.udf.sample.UDFSplit", 
    "type" = "StarrocksJar", 
    "file" = "http://http_host:http_port/udf-1.0-SNAPSHOT-jar-with-dependencies.jar"
);
```

PROPERTIES 内のパラメーターの説明は、[スカラ UDF を作成](#create-a-scalar-udf) の説明と同じです。

### ステップ 7: UDF を使用

UDF を作成した後、ビジネスニーズに基づいてテストおよび使用できます。

#### スカラ UDF を使用

次のコマンドを実行して、前の例で作成したスカラ UDF を使用します。

```SQL
SELECT MY_UDF_JSON_GET('{"key":"{\\"in\\":2}"}', '$.key.in');
```

#### UDAF を使用

次のコマンドを実行して、前の例で作成した UDAF を使用します。

```SQL
SELECT MY_SUM_INT(col1);
```

#### UDWF を使用

次のコマンドを実行して、前の例で作成した UDWF を使用します。

```SQL
SELECT MY_WINDOW_SUM_INT(intcol) 
            OVER (PARTITION BY intcol2
                  ORDER BY intcol3
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
FROM test_basic;
```

#### UDTF を使用

次のコマンドを実行して、前の例で作成した UDTF を使用します。

```Plain
-- t1 という名前のテーブルがあり、その列 a、b、c1 に関する情報が次のようであると仮定します。
SELECT t1.a,t1.b,t1.c1 FROM t1;
> output:
1,2.1,"hello world"
2,2.2,"hello UDTF."

-- MY_UDF_SPLIT() 関数を実行します。
SELECT t1.a,t1.b, MY_UDF_SPLIT FROM t1, MY_UDF_SPLIT(t1.c1); 
> output:
1,2.1,"hello"
1,2.1,"world"
2,2.2,"hello"
2,2.2,"UDTF."
```

> **NOTE**
>
> - 上記のコードスニペットの最初の `MY_UDF_SPLIT` は、関数である 2 番目の `MY_UDF_SPLIT` によって返される列のエイリアスです。
> - テーブルとその列のエイリアスを指定するために `AS t2(f1)` を使用することはできません。

## UDF を表示

次のコマンドを実行して UDF をクエリします。

```SQL
SHOW [GLOBAL] FUNCTIONS;
```

詳細については、[SHOW FUNCTIONS](../sql-statements/Function/SHOW_FUNCTIONS.md) を参照してください。

## UDF を削除

次のコマンドを実行して UDF を削除します。

```SQL
DROP [GLOBAL] FUNCTION <function_name>(arg_type [, ...]);
```

詳細については、[DROP FUNCTION](../sql-statements/Function/DROP_FUNCTION.md) を参照してください。

## SQL データ型と Java データ型のマッピング

| SQL TYPE       | Java TYPE         |
| -------------- | ----------------- |
| BOOLEAN        | java.lang.Boolean |
| TINYINT        | java.lang.Byte    |
| SMALLINT       | java.lang.Short   |
| INT            | java.lang.Integer |
| BIGINT         | java.lang.Long    |
| FLOAT          | java.lang.Float   |
| DOUBLE         | java.lang.Double  |
| STRING/VARCHAR | java.lang.String  |

## パラメーター設定

StarRocks クラスター内の各 Java 仮想マシン (JVM) の **be/conf/be.conf** ファイルで、次の環境変数を設定してメモリ使用量を制御します。JDK 8 を使用する場合は `JAVA_OPTS` を設定し、JDK 9 以降を使用する場合は `JAVA_OPTS_FOR_JDK_9_AND_LATER` を設定します。

```Bash
JAVA_OPTS="-Xmx12G"

JAVA_OPTS_FOR_JDK_9_AND_LATER="-Xmx12G"
```

## FAQ

UDF を作成する際に静的変数を使用できますか？異なる UDF の静的変数は互いに影響を与えますか？

はい、UDF をコンパイルする際に静的変数を使用できます。異なる UDF の静的変数は互いに分離されており、UDF が同じ名前のクラスを持っていても互いに影響を与えません。