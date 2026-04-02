<?php

declare(strict_types=1);

namespace PyroSQL\Laravel\Tests;

use Illuminate\Database\Connection;
use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Expression;
use Illuminate\Database\Query\Processors\Processor;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Fluent;
use PHPUnit\Framework\TestCase;
use PyroSQL\Laravel\PyroSqlConnection;
use PyroSQL\Laravel\PyroSqlConnector;
use PyroSQL\Laravel\Query\PyroSqlGrammar as QueryGrammar;
use PyroSQL\Laravel\Query\PyroSqlProcessor;
use PyroSQL\Laravel\Schema\PyroSqlBuilder;
use PyroSQL\Laravel\Schema\PyroSqlGrammar as SchemaGrammar;

class GrammarTest extends TestCase
{
    private QueryGrammar $queryGrammar;
    private SchemaGrammar $schemaGrammar;
    private Connection $connection;

    protected function setUp(): void
    {
        $pdo = $this->createMock(\PDO::class);
        $this->connection = $this->createMock(Connection::class);
        $this->connection->method('getTablePrefix')->willReturn('');
        $this->connection->method('getPdo')->willReturn($pdo);

        $this->queryGrammar = new QueryGrammar($this->connection);
        $this->schemaGrammar = new SchemaGrammar($this->connection);

        $this->connection->method('getSchemaGrammar')->willReturn($this->schemaGrammar);

        $schemaBuilder = $this->createMock(\Illuminate\Database\Schema\Builder::class);
        $this->connection->method('getSchemaBuilder')->willReturn($schemaBuilder);
    }

    private function getBuilder(): Builder
    {
        $processor = new PyroSqlProcessor();
        $connection = $this->createMock(Connection::class);
        $connection->method('getQueryGrammar')->willReturn($this->queryGrammar);
        $connection->method('getPostProcessor')->willReturn($processor);
        $connection->method('getTablePrefix')->willReturn('');

        return new Builder($connection, $this->queryGrammar, $processor);
    }

    private function getSchemaBlueprint(string $table): Blueprint
    {
        return new Blueprint($this->connection, $table);
    }

    // ── Query Grammar: SELECT tests ────────────────────────────────────

    public function testBasicSelect(): void
    {
        $builder = $this->getBuilder();
        $builder->select('*')->from('users');

        $this->assertSame('select * from "users"', $builder->toSql());
    }

    public function testSelectWithColumns(): void
    {
        $builder = $this->getBuilder();
        $builder->select('name', 'email')->from('users');

        $this->assertSame('select "name", "email" from "users"', $builder->toSql());
    }

    public function testSelectDistinct(): void
    {
        $builder = $this->getBuilder();
        $builder->distinct()->select('name')->from('users');

        $this->assertSame('select distinct "name" from "users"', $builder->toSql());
    }

    public function testSelectWithWhere(): void
    {
        $builder = $this->getBuilder();
        $builder->select('*')->from('users')->where('id', '=', 1);

        $this->assertSame('select * from "users" where "id" = ?', $builder->toSql());
        $this->assertEquals([1], $builder->getBindings());
    }

    public function testSelectWithMultipleWheres(): void
    {
        $builder = $this->getBuilder();
        $builder->select('*')->from('users')
            ->where('active', '=', true)
            ->where('age', '>', 18);

        $this->assertSame('select * from "users" where "active" = ? and "age" > ?', $builder->toSql());
    }

    public function testSelectWithOrWhere(): void
    {
        $builder = $this->getBuilder();
        $builder->select('*')->from('users')
            ->where('name', '=', 'Alice')
            ->orWhere('name', '=', 'Bob');

        $this->assertSame(
            'select * from "users" where "name" = ? or "name" = ?',
            $builder->toSql(),
        );
    }

    public function testSelectWithWhereIn(): void
    {
        $builder = $this->getBuilder();
        $builder->select('*')->from('users')->whereIn('id', [1, 2, 3]);

        $this->assertSame('select * from "users" where "id" in (?, ?, ?)', $builder->toSql());
    }

    public function testSelectWithWhereNull(): void
    {
        $builder = $this->getBuilder();
        $builder->select('*')->from('users')->whereNull('deleted_at');

        $this->assertSame('select * from "users" where "deleted_at" is null', $builder->toSql());
    }

    public function testSelectWithWhereNotNull(): void
    {
        $builder = $this->getBuilder();
        $builder->select('*')->from('users')->whereNotNull('email');

        $this->assertSame('select * from "users" where "email" is not null', $builder->toSql());
    }

    public function testSelectWithWhereBetween(): void
    {
        $builder = $this->getBuilder();
        $builder->select('*')->from('users')->whereBetween('age', [18, 65]);

        $this->assertSame('select * from "users" where "age" between ? and ?', $builder->toSql());
    }

    // ── Query Grammar: LIMIT / OFFSET ──────────────────────────────────

    public function testSelectWithLimit(): void
    {
        $builder = $this->getBuilder();
        $builder->select('*')->from('users')->limit(10);

        $this->assertSame('select * from "users" limit 10', $builder->toSql());
    }

    public function testSelectWithLimitAndOffset(): void
    {
        $builder = $this->getBuilder();
        $builder->select('*')->from('users')->limit(10)->offset(20);

        $this->assertSame('select * from "users" limit 10 offset 20', $builder->toSql());
    }

    public function testSelectWithOffsetOnly(): void
    {
        $builder = $this->getBuilder();
        $builder->select('*')->from('users')->offset(5);

        $sql = $builder->toSql();
        $this->assertStringContainsString('offset 5', $sql);
    }

    // ── Query Grammar: ORDER BY ────────────────────────────────────────

    public function testSelectWithOrderBy(): void
    {
        $builder = $this->getBuilder();
        $builder->select('*')->from('users')->orderBy('name', 'asc');

        $this->assertSame('select * from "users" order by "name" asc', $builder->toSql());
    }

    public function testSelectWithMultipleOrderBy(): void
    {
        $builder = $this->getBuilder();
        $builder->select('*')->from('users')
            ->orderBy('name', 'asc')
            ->orderBy('created_at', 'desc');

        $this->assertSame(
            'select * from "users" order by "name" asc, "created_at" desc',
            $builder->toSql(),
        );
    }

    // ── Query Grammar: GROUP BY / HAVING ───────────────────────────────

    public function testSelectWithGroupBy(): void
    {
        $builder = $this->getBuilder();
        $builder->select('status', new Expression('count(*) as cnt'))
            ->from('orders')
            ->groupBy('status');

        $this->assertSame(
            'select "status", count(*) as cnt from "orders" group by "status"',
            $builder->toSql(),
        );
    }

    public function testSelectWithHaving(): void
    {
        $builder = $this->getBuilder();
        $builder->select('status', new Expression('count(*) as cnt'))
            ->from('orders')
            ->groupBy('status')
            ->having(new Expression('count(*)'), '>', 5);

        $sql = $builder->toSql();
        $this->assertStringContainsString('group by "status"', $sql);
        $this->assertStringContainsString('having count(*) > ?', $sql);
    }

    // ── Query Grammar: JOINS ───────────────────────────────────────────

    public function testInnerJoin(): void
    {
        $builder = $this->getBuilder();
        $builder->select('*')->from('users')
            ->join('orders', 'users.id', '=', 'orders.user_id');

        $this->assertSame(
            'select * from "users" inner join "orders" on "users"."id" = "orders"."user_id"',
            $builder->toSql(),
        );
    }

    public function testLeftJoin(): void
    {
        $builder = $this->getBuilder();
        $builder->select('*')->from('users')
            ->leftJoin('orders', 'users.id', '=', 'orders.user_id');

        $this->assertSame(
            'select * from "users" left join "orders" on "users"."id" = "orders"."user_id"',
            $builder->toSql(),
        );
    }

    // ── Query Grammar: INSERT ──────────────────────────────────────────

    public function testInsert(): void
    {
        $builder = $this->getBuilder();
        $builder->from('users');

        $sql = $this->queryGrammar->compileInsert($builder, [
            ['name' => 'Alice', 'email' => 'alice@example.com'],
        ]);

        $this->assertSame(
            'insert into "users" ("name", "email") values (?, ?)',
            $sql,
        );
    }

    public function testInsertMultipleRows(): void
    {
        $builder = $this->getBuilder();
        $builder->from('users');

        $sql = $this->queryGrammar->compileInsert($builder, [
            ['name' => 'Alice', 'email' => 'alice@example.com'],
            ['name' => 'Bob', 'email' => 'bob@example.com'],
        ]);

        $this->assertSame(
            'insert into "users" ("name", "email") values (?, ?), (?, ?)',
            $sql,
        );
    }

    public function testInsertGetIdWithReturning(): void
    {
        $builder = $this->getBuilder();
        $builder->from('users');

        $sql = $this->queryGrammar->compileInsertGetId($builder, [
            'name' => 'Alice',
            'email' => 'alice@example.com',
        ], 'id');

        $this->assertStringContainsString('insert into "users"', $sql);
        $this->assertStringEndsWith('returning "id"', $sql);
    }

    public function testInsertGetIdWithCustomSequence(): void
    {
        $builder = $this->getBuilder();
        $builder->from('orders');

        $sql = $this->queryGrammar->compileInsertGetId($builder, [
            'total' => 99.99,
        ], 'order_id');

        $this->assertStringEndsWith('returning "order_id"', $sql);
    }

    public function testInsertGetIdDefaultSequence(): void
    {
        $builder = $this->getBuilder();
        $builder->from('users');

        $sql = $this->queryGrammar->compileInsertGetId($builder, [
            'name' => 'Alice',
        ], null);

        $this->assertStringEndsWith('returning "id"', $sql);
    }

    public function testInsertOrIgnore(): void
    {
        $builder = $this->getBuilder();
        $builder->from('users');

        $sql = $this->queryGrammar->compileInsertOrIgnore($builder, [
            ['name' => 'Alice', 'email' => 'alice@example.com'],
        ]);

        $this->assertStringContainsString('insert into "users"', $sql);
        $this->assertStringEndsWith('on conflict do nothing', $sql);
    }

    // ── Query Grammar: UPSERT ──────────────────────────────────────────

    public function testUpsert(): void
    {
        $builder = $this->getBuilder();
        $builder->from('users');

        $sql = $this->queryGrammar->compileUpsert(
            $builder,
            [['email' => 'a@b.com', 'name' => 'A']],
            ['email'],
            ['name'],
        );

        $this->assertStringContainsString('insert into "users"', $sql);
        $this->assertStringContainsString('on conflict ("email") do update set', $sql);
        $this->assertStringContainsString('"name" = "excluded"."name"', $sql);
    }

    // ── Query Grammar: UPDATE ──────────────────────────────────────────

    public function testUpdate(): void
    {
        $builder = $this->getBuilder();
        $builder->from('users')->where('id', '=', 1);

        $sql = $this->queryGrammar->compileUpdate($builder, ['name' => 'Alice']);

        $this->assertSame(
            'update "users" set "name" = ? where "id" = ?',
            $sql,
        );
    }

    // ── Query Grammar: DELETE ──────────────────────────────────────────

    public function testDelete(): void
    {
        $builder = $this->getBuilder();
        $builder->from('users')->where('id', '=', 1);

        $sql = $this->queryGrammar->compileDelete($builder);

        $this->assertSame('delete from "users" where "id" = ?', $sql);
    }

    // ── Query Grammar: TRUNCATE ────────────────────────────────────────

    public function testTruncate(): void
    {
        $builder = $this->getBuilder();
        $builder->from('users');

        $result = $this->queryGrammar->compileTruncate($builder);

        $this->assertArrayHasKey('truncate "users" restart identity cascade', $result);
    }

    // ── Query Grammar: LOCKING ─────────────────────────────────────────

    public function testSelectForUpdate(): void
    {
        $builder = $this->getBuilder();
        $builder->select('*')->from('users')->where('id', '=', 1)->lock(true);

        $this->assertStringEndsWith('for update', $builder->toSql());
    }

    public function testSelectForShare(): void
    {
        $builder = $this->getBuilder();
        $builder->select('*')->from('users')->where('id', '=', 1)->lock(false);

        $this->assertStringEndsWith('for share', $builder->toSql());
    }

    public function testSelectWithCustomLock(): void
    {
        $builder = $this->getBuilder();
        $builder->select('*')->from('users')->lock('for update skip locked');

        $this->assertStringEndsWith('for update skip locked', $builder->toSql());
    }

    // ── Query Grammar: Identifier quoting ──────────────────────────────

    public function testIdentifierQuoting(): void
    {
        $builder = $this->getBuilder();
        $builder->select('name')->from('users')->where('users.id', '=', 1);

        $sql = $builder->toSql();
        $this->assertStringContainsString('"users"."id"', $sql);
        $this->assertStringContainsString('"name"', $sql);
    }

    public function testReservedWordQuoting(): void
    {
        $this->assertSame('"order"', $this->queryGrammar->wrap('order'));
        $this->assertSame('"select"', $this->queryGrammar->wrap('select'));
        $this->assertSame('"table"', $this->queryGrammar->wrap('table'));
    }

    public function testStarNotQuoted(): void
    {
        $builder = $this->getBuilder();
        $builder->select('*')->from('users');

        $this->assertSame('select * from "users"', $builder->toSql());
    }

    // ── Query Grammar: Aggregates ──────────────────────────────────────

    public function testCount(): void
    {
        $builder = $this->getBuilder();
        $builder->from('users');

        $builder->aggregate = ['function' => 'count', 'columns' => ['*']];
        $sql = $this->queryGrammar->compileSelect($builder);

        $this->assertSame('select count(*) as aggregate from "users"', $sql);
    }

    public function testSum(): void
    {
        $builder = $this->getBuilder();
        $builder->from('orders');

        $builder->aggregate = ['function' => 'sum', 'columns' => ['total']];
        $sql = $this->queryGrammar->compileSelect($builder);

        $this->assertSame('select sum("total") as aggregate from "orders"', $sql);
    }

    // ── Query Grammar: Subqueries ──────────────────────────────────────

    public function testWhereExists(): void
    {
        $builder = $this->getBuilder();
        $builder->select('*')->from('users')
            ->whereExists(function ($query) {
                $query->select(new Expression('1'))
                    ->from('orders')
                    ->whereColumn('orders.user_id', 'users.id');
            });

        $sql = $builder->toSql();
        $this->assertStringContainsString('where exists', $sql);
        $this->assertStringContainsString('"orders"."user_id"', $sql);
    }

    // ── Query Grammar: Savepoints ──────────────────────────────────────

    public function testSavepoints(): void
    {
        $this->assertTrue($this->queryGrammar->supportsSavepoints());

        $this->assertSame('SAVEPOINT sp1', $this->queryGrammar->compileSavepoint('sp1'));
        $this->assertSame(
            'ROLLBACK TO SAVEPOINT sp1',
            $this->queryGrammar->compileSavepointRollBack('sp1'),
        );
    }

    // ── Schema Grammar: CREATE TABLE ───────────────────────────────────

    public function testCreateTableBasic(): void
    {
        $blueprint = $this->getSchemaBlueprint('users');
        $blueprint->increments('id');
        $blueprint->string('name');
        $blueprint->string('email', 100);

        $sql = $this->schemaGrammar->compileCreate(
            $blueprint,
            new Fluent(['name' => 'create']),
        );

        $this->assertStringContainsString('create table "users"', $sql);
        $this->assertStringContainsString('"id" serial primary key', $sql);
        $this->assertStringContainsString('"name" varchar(255) not null', $sql);
        $this->assertStringContainsString('"email" varchar(100) not null', $sql);
    }

    public function testCreateTableWithSerialBigInteger(): void
    {
        $blueprint = $this->getSchemaBlueprint('events');
        $blueprint->bigIncrements('id');
        $blueprint->text('payload');

        $sql = $this->schemaGrammar->compileCreate(
            $blueprint,
            new Fluent(['name' => 'create']),
        );

        $this->assertStringContainsString('"id" bigserial primary key', $sql);
        $this->assertStringContainsString('"payload" text not null', $sql);
    }

    public function testCreateTableWithAllColumnTypes(): void
    {
        $blueprint = $this->getSchemaBlueprint('all_types');
        $blueprint->increments('id');
        $blueprint->string('name', 200);
        $blueprint->text('bio');
        $blueprint->integer('age');
        $blueprint->bigInteger('big_num');
        $blueprint->smallInteger('small_num');
        $blueprint->tinyInteger('tiny_num');
        $blueprint->float('score');
        $blueprint->double('precise_score');
        $blueprint->decimal('price', 10, 2);
        $blueprint->boolean('active');
        $blueprint->date('birth_date');
        $blueprint->dateTime('created_at');
        $blueprint->dateTimeTz('event_at');
        $blueprint->time('alarm_time');
        $blueprint->timestamp('logged_at');
        $blueprint->timestampTz('scheduled_at');
        $blueprint->binary('avatar');
        $blueprint->uuid('uuid_col');
        $blueprint->json('metadata');
        $blueprint->jsonb('settings');
        $blueprint->char('code', 6);

        $sql = $this->schemaGrammar->compileCreate(
            $blueprint,
            new Fluent(['name' => 'create']),
        );

        $this->assertStringContainsString('"id" serial', $sql);
        $this->assertStringContainsString('"name" varchar(200)', $sql);
        $this->assertStringContainsString('"bio" text', $sql);
        $this->assertStringContainsString('"age" integer', $sql);
        $this->assertStringContainsString('"big_num" bigint', $sql);
        $this->assertStringContainsString('"small_num" smallint', $sql);
        $this->assertStringContainsString('"tiny_num" smallint', $sql);
        // L12 Blueprint float() defaults to precision=53, producing float(53)
        $this->assertStringContainsString('"score" float(', $sql);
        $this->assertStringContainsString('"precise_score" double precision', $sql);
        $this->assertStringContainsString('"price" numeric(10, 2)', $sql);
        $this->assertStringContainsString('"active" boolean', $sql);
        $this->assertStringContainsString('"birth_date" date', $sql);
        $this->assertStringContainsString('"created_at" timestamp(0)', $sql);
        $this->assertStringContainsString('"event_at" timestamp(0) with time zone', $sql);
        $this->assertStringContainsString('"alarm_time" time(0)', $sql);
        $this->assertStringContainsString('"logged_at" timestamp(0)', $sql);
        $this->assertStringContainsString('"scheduled_at" timestamp(0) with time zone', $sql);
        $this->assertStringContainsString('"avatar" bytea', $sql);
        $this->assertStringContainsString('"uuid_col" uuid', $sql);
        $this->assertStringContainsString('"metadata" jsonb', $sql);
        $this->assertStringContainsString('"settings" jsonb', $sql);
        $this->assertStringContainsString('"code" char(6)', $sql);
    }

    public function testCreateTableWithNullable(): void
    {
        $blueprint = $this->getSchemaBlueprint('profiles');
        $blueprint->increments('id');
        $blueprint->string('nickname')->nullable();
        $blueprint->text('bio')->nullable();

        $sql = $this->schemaGrammar->compileCreate(
            $blueprint,
            new Fluent(['name' => 'create']),
        );

        $this->assertStringContainsString('"nickname" varchar(255) null', $sql);
        $this->assertStringContainsString('"bio" text null', $sql);
    }

    public function testCreateTableWithDefaults(): void
    {
        $blueprint = $this->getSchemaBlueprint('settings');
        $blueprint->increments('id');
        $blueprint->boolean('active')->default(true);
        $blueprint->integer('retries')->default(3);
        $blueprint->string('locale')->default('en');

        $sql = $this->schemaGrammar->compileCreate(
            $blueprint,
            new Fluent(['name' => 'create']),
        );

        // Laravel normalizes boolean true to '1' through getDefaultValue()
        $this->assertStringContainsString("default '1'", $sql);
        $this->assertStringContainsString("default '3'", $sql);
        $this->assertStringContainsString("default 'en'", $sql);
    }

    public function testCreateTableWithEnum(): void
    {
        $blueprint = $this->getSchemaBlueprint('tasks');
        $blueprint->increments('id');
        $blueprint->enum('status', ['pending', 'active', 'done']);

        $sql = $this->schemaGrammar->compileCreate(
            $blueprint,
            new Fluent(['name' => 'create']),
        );

        $this->assertStringContainsString("varchar(255) check", $sql);
        $this->assertStringContainsString("'pending'", $sql);
        $this->assertStringContainsString("'active'", $sql);
        $this->assertStringContainsString("'done'", $sql);
    }

    // ── Schema Grammar: ALTER TABLE ────────────────────────────────────

    public function testDropColumn(): void
    {
        $blueprint = $this->getSchemaBlueprint('users');

        $sql = $this->schemaGrammar->compileDropColumn(
            $blueprint,
            new Fluent(['name' => 'dropColumn', 'columns' => ['phone', 'fax']]),
        );

        $this->assertStringContainsString('alter table "users"', $sql);
        $this->assertStringContainsString('drop column "phone"', $sql);
        $this->assertStringContainsString('drop column "fax"', $sql);
    }

    // ── Schema Grammar: INDEX ──────────────────────────────────────────

    public function testCreateIndex(): void
    {
        $blueprint = $this->getSchemaBlueprint('users');

        $sql = $this->schemaGrammar->compileIndex(
            $blueprint,
            new Fluent(['name' => 'index', 'index' => 'users_email_index', 'columns' => ['email']]),
        );

        $this->assertSame(
            'create index "users_email_index" on "users" ("email")',
            $sql,
        );
    }

    public function testCreateUniqueIndex(): void
    {
        $blueprint = $this->getSchemaBlueprint('users');

        $sql = $this->schemaGrammar->compileUnique(
            $blueprint,
            new Fluent(['name' => 'unique', 'index' => 'users_email_unique', 'columns' => ['email']]),
        );

        $this->assertSame(
            'alter table "users" add constraint "users_email_unique" unique ("email")',
            $sql,
        );
    }

    public function testDropIndex(): void
    {
        $blueprint = $this->getSchemaBlueprint('users');

        $sql = $this->schemaGrammar->compileDropIndex(
            $blueprint,
            new Fluent(['name' => 'dropIndex', 'index' => 'users_email_index']),
        );

        $this->assertSame('drop index "users_email_index"', $sql);
    }

    public function testCreateCompositeIndex(): void
    {
        $blueprint = $this->getSchemaBlueprint('orders');

        $sql = $this->schemaGrammar->compileIndex(
            $blueprint,
            new Fluent(['name' => 'index', 'index' => 'orders_user_status', 'columns' => ['user_id', 'status']]),
        );

        $this->assertSame(
            'create index "orders_user_status" on "orders" ("user_id", "status")',
            $sql,
        );
    }

    // ── Schema Grammar: FOREIGN KEY ────────────────────────────────────

    public function testDropForeignKey(): void
    {
        $blueprint = $this->getSchemaBlueprint('orders');

        $sql = $this->schemaGrammar->compileDropForeign(
            $blueprint,
            new Fluent(['name' => 'dropForeign', 'index' => 'orders_user_id_foreign']),
        );

        $this->assertSame(
            'alter table "orders" drop constraint "orders_user_id_foreign"',
            $sql,
        );
    }

    // ── Schema Grammar: PRIMARY KEY ────────────────────────────────────

    public function testAddPrimaryKey(): void
    {
        $blueprint = $this->getSchemaBlueprint('users');

        $sql = $this->schemaGrammar->compilePrimary(
            $blueprint,
            new Fluent(['name' => 'primary', 'columns' => ['id']]),
        );

        $this->assertSame('alter table "users" add primary key ("id")', $sql);
    }

    public function testDropPrimaryKey(): void
    {
        $blueprint = $this->getSchemaBlueprint('users');

        $sql = $this->schemaGrammar->compileDropPrimary(
            $blueprint,
            new Fluent(['name' => 'dropPrimary']),
        );

        $this->assertStringContainsString('drop constraint', $sql);
        $this->assertStringContainsString('pkey', $sql);
    }

    // ── Schema Grammar: DROP TABLE ─────────────────────────────────────

    public function testDropTable(): void
    {
        $blueprint = $this->getSchemaBlueprint('users');

        $sql = $this->schemaGrammar->compileDrop(
            $blueprint,
            new Fluent(['name' => 'drop']),
        );

        $this->assertSame('drop table "users"', $sql);
    }

    public function testDropTableIfExists(): void
    {
        $blueprint = $this->getSchemaBlueprint('users');

        $sql = $this->schemaGrammar->compileDropIfExists(
            $blueprint,
            new Fluent(['name' => 'dropIfExists']),
        );

        $this->assertSame('drop table if exists "users"', $sql);
    }

    // ── Schema Grammar: RENAME TABLE ───────────────────────────────────

    public function testRenameTable(): void
    {
        $blueprint = $this->getSchemaBlueprint('users');

        $sql = $this->schemaGrammar->compileRename(
            $blueprint,
            new Fluent(['name' => 'rename', 'to' => 'members']),
        );

        $this->assertSame('alter table "users" rename to "members"', $sql);
    }

    // ── Schema Grammar: RENAME INDEX ───────────────────────────────────

    public function testRenameIndex(): void
    {
        $blueprint = $this->getSchemaBlueprint('users');

        $sql = $this->schemaGrammar->compileRenameIndex(
            $blueprint,
            new Fluent(['name' => 'renameIndex', 'from' => 'old_index', 'to' => 'new_index']),
        );

        $this->assertSame('alter index "old_index" rename to "new_index"', $sql);
    }

    // ── Schema Grammar: FOREIGN KEY CONSTRAINTS ────────────────────────

    public function testEnableForeignKeyConstraints(): void
    {
        $this->assertSame(
            'SET CONSTRAINTS ALL IMMEDIATE;',
            $this->schemaGrammar->compileEnableForeignKeyConstraints(),
        );
    }

    public function testDisableForeignKeyConstraints(): void
    {
        $this->assertSame(
            'SET CONSTRAINTS ALL DEFERRED;',
            $this->schemaGrammar->compileDisableForeignKeyConstraints(),
        );
    }

    // ── Schema Grammar: Introspection queries ──────────────────────────

    public function testCompileTables(): void
    {
        $sql = $this->schemaGrammar->compileTables('public');

        $this->assertStringContainsString('information_schema.tables', $sql);
        $this->assertStringContainsString("table_schema = 'public'", $sql);
        $this->assertStringContainsString("table_type = 'BASE TABLE'", $sql);
    }

    public function testCompileViews(): void
    {
        $sql = $this->schemaGrammar->compileViews('public');

        $this->assertStringContainsString('information_schema.views', $sql);
        $this->assertStringContainsString("table_schema = 'public'", $sql);
    }

    public function testCompileColumns(): void
    {
        $sql = $this->schemaGrammar->compileColumns('public', 'users');

        $this->assertStringContainsString('information_schema.columns', $sql);
        $this->assertStringContainsString("'users'", $sql);
        $this->assertStringContainsString('column_name', $sql);
        $this->assertStringContainsString('data_type', $sql);
    }

    public function testCompileIndexes(): void
    {
        $sql = $this->schemaGrammar->compileIndexes('public', 'users');

        $this->assertStringContainsString('pg_indexes', $sql);
        $this->assertStringContainsString("'users'", $sql);
    }

    public function testCompileForeignKeys(): void
    {
        $sql = $this->schemaGrammar->compileForeignKeys('public', 'orders');

        $this->assertStringContainsString('table_constraints', $sql);
        $this->assertStringContainsString("'orders'", $sql);
        $this->assertStringContainsString('FOREIGN KEY', $sql);
    }

    // ── Schema Grammar: Drop all ───────────────────────────────────────

    public function testDropAllTables(): void
    {
        $sql = $this->schemaGrammar->compileDropAllTables(['users', 'orders', 'products']);

        $this->assertStringContainsString('drop table', $sql);
        $this->assertStringContainsString('"users"', $sql);
        $this->assertStringContainsString('"orders"', $sql);
        $this->assertStringContainsString('"products"', $sql);
        $this->assertStringContainsString('cascade', $sql);
    }

    public function testDropAllViews(): void
    {
        $sql = $this->schemaGrammar->compileDropAllViews(['user_stats', 'order_summary']);

        $this->assertStringContainsString('drop view', $sql);
        $this->assertStringContainsString('"user_stats"', $sql);
        $this->assertStringContainsString('"order_summary"', $sql);
        $this->assertStringContainsString('cascade', $sql);
    }

    // ── Schema Grammar: Fulltext index ─────────────────────────────────

    public function testFulltextIndex(): void
    {
        $blueprint = $this->getSchemaBlueprint('articles');

        $sql = $this->schemaGrammar->compileFulltext(
            $blueprint,
            new Fluent(['name' => 'fulltext', 'index' => 'articles_body_fulltext', 'columns' => ['body'], 'language' => null]),
        );

        $this->assertStringContainsString('using gin', $sql);
        $this->assertStringContainsString('to_tsvector', $sql);
        $this->assertStringContainsString("'english'", $sql);
    }

    // ── Schema Grammar: Spatial index ──────────────────────────────────

    public function testSpatialIndex(): void
    {
        $blueprint = $this->getSchemaBlueprint('locations');

        $sql = $this->schemaGrammar->compileSpatialIndex(
            $blueprint,
            new Fluent(['name' => 'spatial', 'index' => 'locations_coords', 'columns' => ['coords']]),
        );

        $this->assertStringContainsString('using gist', $sql);
    }

    // ── Connector: DSN building ────────────────────────────────────────

    public function testConnectorBuildsDsn(): void
    {
        $connector = new PyroSqlConnector();
        $reflection = new \ReflectionMethod($connector, 'getDsn');
        $reflection->setAccessible(true);

        $dsn = $reflection->invoke($connector, [
            'host' => '10.0.0.1',
            'port' => 12520,
            'database' => 'mydb',
        ]);

        $this->assertSame('pyrosql:host=10.0.0.1;port=12520;dbname=mydb', $dsn);
    }

    public function testConnectorBuildsDsnWithDefaults(): void
    {
        $connector = new PyroSqlConnector();
        $reflection = new \ReflectionMethod($connector, 'getDsn');
        $reflection->setAccessible(true);

        $dsn = $reflection->invoke($connector, []);

        $this->assertSame('pyrosql:host=127.0.0.1;port=12520;dbname=forge', $dsn);
    }

    // ── Processor: processInsertGetId ──────────────────────────────────

    public function testProcessorExtractsIdFromObject(): void
    {
        $processor = new PyroSqlProcessor();
        $builder = $this->getBuilder();
        $builder->from('users');

        $connection = $this->createMock(Connection::class);
        $connection->method('recordsHaveBeenModified')->willReturn(null);
        $connection->method('selectOne')->willReturn((object) ['id' => 42]);

        $reflection = new \ReflectionProperty($builder, 'connection');
        $reflection->setAccessible(true);
        $reflection->setValue($builder, $connection);

        $id = $processor->processInsertGetId(
            $builder,
            'insert into "users" ("name") values (?) returning "id"',
            ['Alice'],
            'id',
        );

        $this->assertSame(42, $id);
    }

    public function testProcessorExtractsIdFromArray(): void
    {
        $processor = new PyroSqlProcessor();
        $builder = $this->getBuilder();
        $builder->from('users');

        $connection = $this->createMock(Connection::class);
        $connection->method('recordsHaveBeenModified')->willReturn(null);
        $connection->method('selectOne')->willReturn(['order_id' => 99]);

        $reflection = new \ReflectionProperty($builder, 'connection');
        $reflection->setAccessible(true);
        $reflection->setValue($builder, $connection);

        $id = $processor->processInsertGetId(
            $builder,
            'insert into "orders" ("total") values (?) returning "order_id"',
            [49.99],
            'order_id',
        );

        $this->assertSame(99, $id);
    }

    public function testProcessorReturnsZeroOnNull(): void
    {
        $processor = new PyroSqlProcessor();
        $builder = $this->getBuilder();
        $builder->from('users');

        $connection = $this->createMock(Connection::class);
        $connection->method('recordsHaveBeenModified')->willReturn(null);
        $connection->method('selectOne')->willReturn(null);

        $reflection = new \ReflectionProperty($builder, 'connection');
        $reflection->setAccessible(true);
        $reflection->setValue($builder, $connection);

        $id = $processor->processInsertGetId(
            $builder,
            'insert into "users" ("name") values (?) returning "id"',
            ['Alice'],
            'id',
        );

        $this->assertSame(0, $id);
    }

    // ── Processor: processColumnListing ────────────────────────────────

    public function testProcessorProcessesColumnListing(): void
    {
        $processor = new PyroSqlProcessor();

        $results = [
            (object) ['column_name' => 'id'],
            (object) ['column_name' => 'name'],
            (object) ['column_name' => 'email'],
        ];

        $columns = $processor->processColumnListing($results);

        $this->assertSame(['id', 'name', 'email'], $columns);
    }

    // ── Processor: processColumns ──────────────────────────────────────

    public function testProcessorProcessesColumns(): void
    {
        $processor = new PyroSqlProcessor();

        $results = [
            (object) [
                'column_name' => 'id',
                'data_type' => 'integer',
                'is_nullable' => 'NO',
                'default' => "nextval('users_id_seq'::regclass)",
                'collation_name' => null,
                'column_default' => "nextval('users_id_seq'::regclass)",
            ],
            (object) [
                'column_name' => 'email',
                'data_type' => 'character varying',
                'is_nullable' => 'YES',
                'default' => null,
                'collation_name' => 'en_US.utf8',
                'column_default' => null,
            ],
        ];

        $columns = $processor->processColumns($results);

        $this->assertCount(2, $columns);
        $this->assertSame('id', $columns[0]['name']);
        $this->assertSame('integer', $columns[0]['type_name']);
        $this->assertFalse($columns[0]['nullable']);
        $this->assertTrue($columns[0]['auto_increment']);

        $this->assertSame('email', $columns[1]['name']);
        $this->assertTrue($columns[1]['nullable']);
        $this->assertFalse($columns[1]['auto_increment']);
        $this->assertSame('en_US.utf8', $columns[1]['collation']);
    }

    // ── Processor: processIndexes ──────────────────────────────────────

    public function testProcessorProcessesIndexes(): void
    {
        $processor = new PyroSqlProcessor();

        $results = [
            (object) [
                'name' => 'users_pkey',
                'indexname' => 'users_pkey',
                'definition' => 'CREATE UNIQUE INDEX users_pkey ON public.users USING btree (id)',
            ],
            (object) [
                'name' => 'users_email_index',
                'indexname' => 'users_email_index',
                'definition' => 'CREATE INDEX users_email_index ON public.users USING btree (email)',
            ],
        ];

        $indexes = $processor->processIndexes($results);

        $this->assertCount(2, $indexes);

        $this->assertSame('users_pkey', $indexes[0]['name']);
        $this->assertTrue($indexes[0]['primary']);
        $this->assertTrue($indexes[0]['unique']);
        $this->assertSame(['id'], $indexes[0]['columns']);

        $this->assertSame('users_email_index', $indexes[1]['name']);
        $this->assertFalse($indexes[1]['primary']);
        $this->assertFalse($indexes[1]['unique']);
        $this->assertSame(['email'], $indexes[1]['columns']);
    }

    // ── Processor: processForeignKeys ──────────────────────────────────

    public function testProcessorProcessesForeignKeys(): void
    {
        $processor = new PyroSqlProcessor();

        $results = [
            (object) [
                'constraint_name' => 'orders_user_id_foreign',
                'column_name' => 'user_id',
                'foreign_table_name' => 'users',
                'foreign_column_name' => 'id',
            ],
        ];

        $fks = $processor->processForeignKeys($results);

        $this->assertCount(1, $fks);
        $this->assertSame('orders_user_id_foreign', $fks[0]['name']);
        $this->assertSame(['user_id'], $fks[0]['columns']);
        $this->assertSame('users', $fks[0]['foreign_table']);
        $this->assertSame(['id'], $fks[0]['foreign_columns']);
    }

    // ── Query Grammar: whereDate with cast ─────────────────────────────

    public function testWhereDateUsesCast(): void
    {
        $builder = $this->getBuilder();
        $builder->select('*')->from('events')->whereDate('created_at', '=', '2025-01-01');

        $sql = $builder->toSql();
        $this->assertStringContainsString('::date', $sql);
    }

    public function testWhereTimeUsesCast(): void
    {
        $builder = $this->getBuilder();
        $builder->select('*')->from('events')->whereTime('created_at', '=', '12:00:00');

        $sql = $builder->toSql();
        $this->assertStringContainsString('::time', $sql);
    }

    public function testWhereDayUsesExtract(): void
    {
        $builder = $this->getBuilder();
        $builder->select('*')->from('events')->whereDay('created_at', '=', '15');

        $sql = $builder->toSql();
        $this->assertStringContainsString('extract(day from', $sql);
    }

    public function testWhereMonthUsesExtract(): void
    {
        $builder = $this->getBuilder();
        $builder->select('*')->from('events')->whereMonth('created_at', '=', '6');

        $sql = $builder->toSql();
        $this->assertStringContainsString('extract(month from', $sql);
    }

    public function testWhereYearUsesExtract(): void
    {
        $builder = $this->getBuilder();
        $builder->select('*')->from('events')->whereYear('created_at', '=', '2025');

        $sql = $builder->toSql();
        $this->assertStringContainsString('extract(year from', $sql);
    }

    // ── Schema Grammar: timestamps helper ──────────────────────────────

    public function testTimestampsColumns(): void
    {
        $blueprint = $this->getSchemaBlueprint('posts');
        $blueprint->increments('id');
        $blueprint->timestamps();

        $sql = $this->schemaGrammar->compileCreate(
            $blueprint,
            new Fluent(['name' => 'create']),
        );

        $this->assertStringContainsString('"created_at" timestamp', $sql);
        $this->assertStringContainsString('"updated_at" timestamp', $sql);
    }

    // ── Schema Grammar: softDeletes ────────────────────────────────────

    public function testSoftDeletesColumn(): void
    {
        $blueprint = $this->getSchemaBlueprint('posts');
        $blueprint->increments('id');
        $blueprint->softDeletes();

        $sql = $this->schemaGrammar->compileCreate(
            $blueprint,
            new Fluent(['name' => 'create']),
        );

        $this->assertStringContainsString('"deleted_at" timestamp', $sql);
        $this->assertStringContainsString('null', $sql);
    }

    // ── Query Grammar: fulltext where clause ───────────────────────────

    public function testWhereFullText(): void
    {
        $builder = $this->getBuilder();

        $where = [
            'columns' => ['body'],
            'value' => 'search term',
            'options' => ['language' => 'english', 'mode' => 'plain'],
        ];

        $sql = $this->queryGrammar->whereFullText($builder, $where);

        $this->assertStringContainsString('to_tsvector', $sql);
        $this->assertStringContainsString('plainto_tsquery', $sql);
        $this->assertStringContainsString("'english'", $sql);
    }

    public function testWhereFullTextWebsearchMode(): void
    {
        $builder = $this->getBuilder();

        $where = [
            'columns' => ['title', 'body'],
            'value' => 'search term',
            'options' => ['language' => 'english', 'mode' => 'websearch'],
        ];

        $sql = $this->queryGrammar->whereFullText($builder, $where);

        $this->assertStringContainsString('websearch_to_tsquery', $sql);
    }

    // ── Connection: driver name ────────────────────────────────────────

    public function testConnectionDriverName(): void
    {
        $pdo = $this->createMock(\PDO::class);

        $connection = new PyroSqlConnection($pdo, 'testdb', '', []);

        $this->assertSame('pyrosql', $connection->getDriverName());
    }

    public function testConnectionReturnsSchemaBuilder(): void
    {
        $pdo = $this->createMock(\PDO::class);

        $connection = new PyroSqlConnection($pdo, 'testdb', '', []);

        $schemaBuilder = $connection->getSchemaBuilder();

        $this->assertInstanceOf(PyroSqlBuilder::class, $schemaBuilder);
    }

    public function testConnectionReturnsCorrectGrammars(): void
    {
        $pdo = $this->createMock(\PDO::class);

        $connection = new PyroSqlConnection($pdo, 'testdb', '', []);

        $this->assertInstanceOf(QueryGrammar::class, $connection->getQueryGrammar());
        $this->assertInstanceOf(PyroSqlProcessor::class, $connection->getPostProcessor());
    }

    // ── Schema Grammar: SQL injection safety in column names ───────────

    public function testIdentifierEscapesDoubleQuotes(): void
    {
        $builder = $this->getBuilder();
        $builder->select('*')->from('users')->where('na"me', '=', 'test');

        $sql = $builder->toSql();
        $this->assertStringContainsString('"na""me"', $sql);
    }

    // ── Schema Grammar: stored/virtual generated columns ───────────────

    public function testStoredGeneratedColumn(): void
    {
        $blueprint = $this->getSchemaBlueprint('products');
        $blueprint->increments('id');
        $blueprint->decimal('price', 10, 2);
        $blueprint->decimal('tax', 10, 2)->storedAs('price * 0.21');

        $sql = $this->schemaGrammar->compileCreate(
            $blueprint,
            new Fluent(['name' => 'create']),
        );

        $this->assertStringContainsString('generated always as (price * 0.21) stored', $sql);
    }

    // ── Schema Grammar: tables with default schema ─────────────────────

    public function testCompileTablesWithNullSchema(): void
    {
        $sql = $this->schemaGrammar->compileTables(null);

        $this->assertStringContainsString("table_schema = 'public'", $sql);
    }

    public function testCompileColumnsWithNullSchema(): void
    {
        $sql = $this->schemaGrammar->compileColumns(null, 'users');

        $this->assertStringContainsString("table_schema = 'public'", $sql);
        $this->assertStringContainsString("'users'", $sql);
    }
}
