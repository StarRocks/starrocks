---
displayed_sidebar: docs
---

# StarRocks Restful API 標準

## API フォーマット

1. API フォーマットは次のパターンに従います: `/api/{version}/{target-object-access-path}/{action}`。
2. `{version}` は `v{number}` として表され、v1, v2, v3, v4 などがあります。
3. `{target-object-access-path}` は階層的に整理されており、詳細は後述します。
4. `{action}` はオプションであり、API 実装者は可能な限り HTTP METHOD を使用して操作の意味を伝えるべきです。HTTP メソッドのセマンティクスが満たされない場合にのみ、アクションを使用します。例えば、オブジェクトの名前を変更するための HTTP メソッドが利用できない場合などです。

## ターゲットオブジェクトアクセスパスの定義

1. REST API によってアクセスされるターゲットオブジェクトは、階層的なアクセスパスに分類して整理する必要があります。アクセスパスのフォーマットは次の通りです:
```
/primary_categories/primary_object/secondary_categories/secondary_object/.../categories/object
```

catalog, database, table, column の例を挙げると:
```
/catalogs: すべての catalogs を表します。
/catalogs/hive: catalog カテゴリの下にある "hive" という名前の特定の catalog オブジェクトを表します。
/catalogs/hive/databases: "hive" catalog 内のすべての databases を表します。
/catalogs/hive/databases/tpch_100g: "hive" catalog 内の "tpch_100g" という名前の database を表します。
/catalogs/hive/databases/tpch_100g/tables: "tpch_100g" database 内のすべての tables を表します。
/catalogs/hive/databases/tpch_100g/tables/lineitem: tpch_100g.lineitem table を表します。
/catalogs/hive/databases/tpch_100g/tables/lineitem/columns: tpch_100g.lineitem table 内のすべての columns を表します。
/catalogs/hive/databases/tpch_100g/tables/lineitem/columns/l_orderkey: tpch_100g.lineitem table 内の特定の column l_orderkey を表します。
```

2. カテゴリはスネークケースで命名され、最後の単語は複数形です。すべての単語は小文字で、複数の単語はアンダースコア (_) でつながれています。特定のオブジェクトは実際の名前を使用して命名されます。ターゲットオブジェクトの階層関係を明確に定義する必要があります。

## HTTP メソッドの選択

1. GET: 単一のオブジェクトを表示し、特定のカテゴリのすべてのオブジェクトをリストするために GET メソッドを使用します。GET メソッドによるオブジェクトへのアクセスは読み取り専用であり、リクエストボディを提供しません。
```
# database ssb_100g 内のすべての tables をリスト
GET /api/v2/catalogs/default/databases/ssb_100g/tables

# table ssb_100g.lineorder を表示
GET /api/v2/catalogs/default/databases/ssb_100g/tables/lineorder
```

2. POST: オブジェクトを作成するために使用されます。パラメータはリクエストボディを通じて渡されます。冪等性はありません。オブジェクトが既に存在する場合、再作成は失敗し、エラーメッセージを返します。
```
POST /api/v2/catalogs/default/databases/ssb_100g/tables/create -d@create_customer.sql
```

3. PUT: オブジェクトを作成するために使用されます。パラメータはリクエストボディを通じて渡されます。冪等性があります。オブジェクトが既に存在する場合、成功を返します。PUT メソッドは POST メソッドの CREATE IF NOT EXISTS バージョンです。
```
PUT /api/v2/databases/ssb_100g/tables/create -d@create_customer.sql
```

4. DELETE: オブジェクトを削除するために使用されます。リクエストボディを提供しません。削除するオブジェクトが存在しない場合、成功を返します。DELETE メソッドは DROP IF EXISTS セマンティクスを持ちます。
```
DELETE /api/v2/catalogs/default/databases/ssb_100g/tables/customer
```

5. PATCH: オブジェクトを更新するために使用されます。リクエストボディを提供し、変更が必要な部分的な情報のみを含みます。
```
PATCH /api/v2/databases/ssb_100g/tables/customer -d '{"unique_key_constraints": ["c_custkey"]}'
```

## 認証と認可

1. 認証と認可情報は HTTP リクエストヘッダーで渡されます。

## HTTP ステータスコード

1. HTTP ステータスコードは、操作の成功または失敗を示すために REST API によって返されます。
2. 成功した操作のステータスコード (2xx) は次の通りです:

- 200 OK: リクエストが正常に完了したことを示します。オブジェクトの表示/リスト/削除/更新や保留中のタスクのステータスのクエリに使用されます。
- 201 Created: オブジェクトが正常に作成されたことを示します。PUT/POST メソッドに使用されます。レスポンスボディには、後続の表示/リスト/削除/更新のためのオブジェクト URI を含める必要があります。
- 202 Accepted: タスクの送信が成功し、タスクが保留中の状態であることを示します。レスポンスボディには、後続のキャンセル、削除、およびタスクステータスのポーリングのためのタスク URI を含める必要があります。

3. エラーコード (4xx) はクライアントエラーを示します。ユーザーは HTTP リクエストを調整して再試行する必要があります。
- 400 Bad Request: 無効なリクエストパラメータ。
- 401 Unauthorized: 認証情報が不足している、違法な認証情報、認証失敗。
- 403 Forbidden: 認証は成功したが、ユーザーの操作が認可チェックに失敗しました。アクセス権がありません。
- 404 Not Found: API URI エンコーディングエラー。登録された REST API に属していません。
- 405 Method Not Allowed: 不正な HTTP メソッドが使用されました。
- 406 Not Acceptable: レスポンスフォーマットが Accept ヘッダーで指定されたメディアタイプと一致しません。
- 415 Not Acceptable: リクエストコンテンツのメディアタイプが Content-Type ヘッダーで指定されたメディアタイプと一致しません。

4. エラーコード (5xx) はサーバーエラーを示します。ユーザーはリクエストを変更する必要はなく、後で再試行できます。
- 500 Internal Server Error: 内部サーバーエラー、未知のエラーに類似。
- 503 Service Unavailable: サービスが一時的に利用できません。例えば、ユーザーのアクセス頻度が高すぎてレート制限に達した場合や、内部ステータスのためにサービスが現在提供できない場合など。例えば、3 レプリカでテーブルを作成しようとしているが、2 つの BEs しか利用できない場合や、ユーザーのクエリに関与するすべての Tablet レプリカが利用できない場合。

## HTTP レスポンスフォーマット

1. API が 200/201/202 の HTTP コードを返す場合、HTTP レスポンスは空ではありません。API は JSON フォーマットで結果を返し、トップレベルのフィールド "code"、"message"、および "result" を含みます。すべての JSON フィールドはキャメルケースで命名されます。

2. 成功した API レスポンスでは、"code" は "0"、"message" は "OK"、"result" は実際の結果を含みます。
```json
{
   "code":"0",
   "message": "OK",
   "result": {....}
}
```

3. 失敗した API レスポンスでは、"code" は "0" ではなく、"message" は簡単なエラーメッセージであり、"result" はエラースタックトレースなどの詳細なエラー情報を含むことができます。
```json
{
   "code":"1",
   "message": "Analyze error",
   "result": {....}
}
```

## パラメータの渡し方

1. API パラメータは、パス、リクエストボディ、クエリパラメータ、およびヘッダーの優先順位で渡されます。パラメータを渡すための適切な方法を選択してください。

2. パスパラメータ: オブジェクトの階層関係を表す必須パラメータは、パスパラメータに配置されます。
```
 /api/v2/warehouses/{warehouseName}/backends/{backendId}
 /api/v2/warehouses/ware0/backends/10027
```

3. リクエストボディ: パラメータは application/json を使用して渡されます。パラメータは必須またはオプションのタイプであることができます。

4. クエリパラメータ: クエリパラメータとリクエストボディパラメータを同時に使用することはできません。同じ API では、どちらか一方を選択してください。ヘッダーパラメータとパスパラメータを除くパラメータの数が 2 以下の場合、クエリパラメータを使用できます。それ以外の場合は、リクエストボディを使用してパラメータを渡します。

5. HEADER パラメータ: ヘッダーは、Content-type や Accept などの HTTP 標準パラメータを渡すために使用されるべきであり、実装者はカスタマイズされたパラメータを渡すために HTTP ヘッダーを乱用すべきではありません。ユーザー拡張のためにヘッダーを使用してパラメータを渡す場合、ヘッダー名は `x-starrocks-{name}` の形式であるべきであり、名前には複数の英単語を含めることができ、各単語は小文字でハイフン (-) で連結されます。