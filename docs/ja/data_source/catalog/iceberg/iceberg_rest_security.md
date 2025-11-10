---
displayed_sidebar: docs
---

# Iceberg REST Catalog のセキュリティ設定

複数のノードが共同で同じデータレイクにアクセスするシナリオでは、**安全で統一された監査可能な権限管理をどのように実現するか**が核心的な課題です。従来のモデルでは、ユーザーは各ノードごとにストレージ資格情報を設定し、ローカルで権限を管理する必要があり、これによりメンテナンスコストが増加し、資格情報の漏洩や権限の不一致のリスクが生じます。

Iceberg REST Catalog を StarRocks と統合することで、JWT (JSON Web Token) 認証と Vended Credentials (一時的な資格情報) を組み合わせて、安全なアクセスと統一された権限管理を実現できます。この設定により以下のことが可能になります：

- **資格情報のリスクを軽減**: StarRocks 内に高権限のアカウント情報を保存する必要がありません。資格情報は Catalog によって一時的に発行され、漏洩を防ぎます。
- **統一された簡素な権限管理**: すべてのデータベース、テーブル、ビューのアクセス制御は Catalog によって一元管理され、異なるノード間での一貫性を確保し、冗長な設定を回避します。
- **コンプライアンスと簡素化された運用**: ユーザーの操作は追跡可能であり、監査を容易にします。また、StarRocks 内での権限とストレージ資格情報の維持コストを削減します。

## セキュリティメカニズム

- **JWT 認証**
  - ユーザーが StarRocks にログインすると、取得した JWT トークンを Iceberg REST Session Catalog に渡すことができます。
  - Catalog は JWT トークンに基づいてユーザーを認証し、実際のユーザーのアイデンティティでクエリを実行します。
  - これにより、StarRocks が高権限のアカウントを保存する必要がなくなり、セキュリティリスクが大幅に低下します。
- **Vended Credentials**
  - 認証後、Catalog はユーザーのために一時的なストレージアクセス資格情報を生成できます。
  - ユーザーは StarRocks 内でストレージ層の資格情報を設定する必要がありません。
  - オブジェクトストレージにアクセスするたびに、StarRocks は Catalog によって発行された一時的な資格情報を使用します。
  - これにより、セキュリティが向上し、資格情報管理が簡素化されます。

## 使用方法

### ステップ 1. JWT 認証の設定

StarRocks で、**[JWT ベースのセキュリティ統合](../../../administration/user_privs/authentication/security_integration.md#create-a-security-integration-with-jwt)** を設定するか、[JWT 認証を使用してユーザーを作成](https://docs.starrocks.io/en/docs/administration/user_privs/authentication/jwt_authentication/)します。

### ステップ 2. Iceberg REST Catalog を作成し、セキュリティ設定を構成

```SQL
CREATE EXTERNAL CATALOG iceberg_rest_catalog
PROPERTIES (
  "iceberg.catalog.type" = "rest",
  "iceberg.catalog.uri" = "<rest_server_api_endpoint>",
  "iceberg.catalog.security" = "jwt",
  "iceberg.catalog.warehouse" = "<identifier_or_path_to_warehouse>",
  "iceberg.catalog.vended-credentials-enabled" = "true"
);
```

プロパティ:

- `iceberg.catalog.type`: このプロパティを `rest` に設定し、REST Catalog を使用することを示します。
- `iceberg.catalog.uri`: REST Catalog サービスの API エンドポイント。
- `iceberg.catalog.security`: このプロパティを `jwt` に設定して JWT 認証を有効にします。StarRocks は現在のユーザーの認証情報を Catalog に渡します。
- `iceberg.catalog.warehouse`: Iceberg データウェアハウスのパスまたは識別子を指定します。
- `iceberg.catalog.vended-credentials-enabled`: このプロパティを `true` に設定して Vended Credentials を有効にし、一時的な資格情報の発行を許可します。

### ステップ 3. 権限を付与

ユーザーが StarRocks 経由で Catalog をクエリする場合、権限は2つのレベルで処理されます：

1. **StarRocks 内部オブジェクト権限**

   ユーザーが StarRocks 内で Catalog オブジェクトを表示する権限を持ち、`SET CATALOG` を使用してその Catalog にセッションを切り替えることができるかどうか。

2. **Catalog 内部権限**

   ユーザーが Catalog 内のデータベース、テーブル、またはビューにアクセスする際に適切なアクセス権限を持っているかどうか。

理想的なワークフローでは、StarRocks はユーザーが StarRocks クラスターにログインした後に Catalog オブジェクトにアクセスするために必要な基本的な権限を管理します。Catalog 内の細かいデータアクセス権限は完全に Catalog 自体によって管理されます。つまり：

- ユーザーは StarRocks 内で Catalog オブジェクトに `USAGE` 権限を持っている必要があり、`SET CATALOG` を使用してカタログに切り替えたり、`SHOW CATALOGS` を使用してカタログ情報を表示したりできます。
- データレベルの権限チェックは完全に Catalog によって処理されます。

#### StarRocks 内部 Catalog オブジェクト権限

StarRocks で `SHOW CATALOGS` または `SET CATALOG` を実行するには、ユーザーは対応する Catalog オブジェクトに `USAGE` 権限が必要です。

##### オプション 1: すべてのユーザーにアクセスを許可

すべてのユーザーが指定された Catalog を表示および切り替えることを許可する場合、`public` ロールに Catalog の `USAGE` 権限を付与できます。

```SQL
GRANT USAGE ON CATALOG <catalog_name> TO ROLE public;
```

これにより、StarRocks にログインしたすべてのユーザーに `public` ロールが自動的に付与され、その Catalog の `USAGE` 権限が与えられます。

##### オプション 2: 細かい権限管理

特定のユーザーまたはグループのみが Catalog を表示および切り替えることを許可する場合、**Group Provider** と **Role** を使用してより細かい管理を行うことができます：

1. [Group Provider](../../../administration/user_privs/group_provider.md) を使用して外部ユーザーグループ情報を StarRocks に同期します。
2. 対応する StarRocks ロールを作成します。

    ```SQL
    CREATE ROLE <role_name>;
    ```

3. ロールを外部ユーザーグループにバインドします：

    ```SQL
    GRANT <role_name> TO EXTERNAL GROUP <group_name>;
    ```

4. ロールに Catalog の `USAGE` 権限を付与します：

    ```SQL
    GRANT USAGE ON CATALOG <catalog_name> TO ROLE <role_name>;
    ```

この場合、外部グループのメンバーは StarRocks にログインすると自動的に割り当てられたロールを継承し、その Catalog の `USAGE` 権限を取得します。

#### Catalog 内部データ権限

Catalog のプロパティを設定することで、Catalog 内のすべてのオブジェクトに対する権限チェックは Catalog 自体に委任されます。Catalog は認証と権限を一元管理し、StarRocks での権限設定を簡素化し、冗長な認可を回避し、異なるエンジン間で一貫した権限ルールを確保します。

```SQL
ALTER CATALOG iceberg_rest_catalog
SET PROPERTIES (
  "catalog.access.control" = "allowall"
);
```

プロパティ:

- `catalog.access.control`: このプロパティを `allowall` に設定します。StarRocks は Catalog 内のオブジェクトに対してさらなる権限チェックを行いません。代わりに、すべての権限管理は Catalog 自体によって処理されます。