# Getting started

**Prerequisites:** Docker with Compose v2, Go **1.27** or newer (see
`go.mod`, only needed for the tests), `make` and `curl`.

```sh
git clone <this repository> && cd twinhouse
make up        # two "regions": us on :8123, eu on :8124
```

`make up` builds one image and starts the local multi-region stack
(`deploy/local/docker-compose.yml`). The stack has two DynamoDB Local instances
(us-east-1 and eu-west-1), a `chserver` and an `mvworker` per region, and
the replicator emulating global tables between them. `make up-single`
starts a single region without replication on `:8125`. `make down` removes
everything.

## Create a source table, a rollup and a materialized view (in us)

```sh
US=http://localhost:8123 EU=http://localhost:8124

curl -sS $US --data-binary "CREATE DATABASE demo"

curl -sS $US --data-binary "
CREATE TABLE demo.page_views (
    ts      DateTime64(3),
    site    LowCardinality(String),
    path    String,
    user_id UInt64
) ENGINE = MergeTree ORDER BY (site, ts)"

curl -sS $US --data-binary "
CREATE TABLE demo.daily_site_stats (
    day   Date,
    site  String,
    views SimpleAggregateFunction(sum, UInt64),
    users AggregateFunction(uniqExact, UInt64)
) ENGINE = AggregatingMergeTree ORDER BY (site, day)"

curl -sS $US --data-binary "
CREATE MATERIALIZED VIEW demo.daily_site_stats_mv TO demo.daily_site_stats AS
SELECT toDate(ts) AS day, site, count() AS views, uniqExactState(user_id) AS users
FROM demo.page_views GROUP BY day, site"
```

## Write in us, read the rollup immediately

```sh
curl -sS $US --data-binary "
INSERT INTO demo.page_views VALUES
  ('2026-10-01 10:00:00.000', 'example.org', '/',     1),
  ('2026-10-01 10:05:00.000', 'example.org', '/docs', 2),
  ('2026-10-01 11:00:00.000', 'example.org', '/',     1)"

curl -sS $US --data-binary "
SELECT day, site, sum(views) AS views, uniqExactMerge(users) AS users
FROM demo.daily_site_stats GROUP BY day, site ORDER BY day, site
FORMAT PrettyCompact"
```

```
   ┌────────day─┬─site────────┬─views─┬─users─┐
1. │ 2026-10-01 │ example.org │     3 │     2 │
   └────────────┴─────────────┴───────┴───────┘
```

## Read it in eu, write in eu, read it back in us

The schema, the source rows and (through eu's own `mvworker`) eu's rollup
arrive within a few seconds:

```sh
sleep 3
curl -sS $EU --data-binary "SELECT sum(views), uniqExactMerge(users) FROM demo.daily_site_stats"
# 3	2

curl -sS "$EU/?query=INSERT%20INTO%20demo.page_views%20FORMAT%20JSONEachRow" --data-binary \
  '{"ts":"2026-10-01 12:00:00.000","site":"example.org","path":"/blog","user_id":3}'

sleep 3
curl -sS $US --data-binary "SELECT sum(views), uniqExactMerge(users) FROM demo.daily_site_stats"
# 4	3
```

## Latest-wins snapshots

```sh
curl -sS $US --data-binary "
CREATE TABLE demo.users (user_id UInt64, plan String, updated_at DateTime64(3))
ENGINE = ReplacingMergeTree(updated_at) ORDER BY user_id"
sleep 3   # let the DDL reach eu

curl -sS $US --data-binary "INSERT INTO demo.users VALUES (1, 'free',  '2026-10-01 10:00:00')"
curl -sS $EU --data-binary "INSERT INTO demo.users VALUES (1, 'pro',   '2026-10-01 11:00:00')"
curl -sS $US --data-binary "INSERT INTO demo.users VALUES (1, 'stale', '2026-10-01 09:00:00')"

sleep 3
curl -sS $US --data-binary "SELECT * FROM demo.users"   # 1	pro	2026-10-01 11:00:00.000
curl -sS $EU --data-binary "SELECT * FROM demo.users"   # 1	pro	2026-10-01 11:00:00.000
```

Query parameters, URL settings, `default_format`, sessions, HTTP Basic or
`X-ClickHouse-User`/`X-ClickHouse-Key` auth and gzip/zstd/lz4/br
compression work as in ClickHouse:

```sh
curl -sS "$US/?param_site=example.org" \
  --data-binary "SELECT count() FROM demo.page_views WHERE site = {site:String}"
```

## From Go (clickhouse-go v2)

Use the **HTTP** protocol. The native TCP protocol (port 9000) is not
implemented.

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func main() {
	ctx := context.Background()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Protocol: clickhouse.HTTP,
		Addr:     []string{"localhost:8123"}, // the region's chserver
		Auth:     clickhouse.Auth{Database: "demo", Username: "default"},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO page_views")
	if err != nil {
		log.Fatal(err)
	}
	if err := batch.Append(time.Now(), "example.org", "/pricing", uint64(42)); err != nil {
		log.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		log.Fatal(err)
	}

	rows, err := conn.Query(ctx, `
		SELECT day, sum(views), uniqExactMerge(users)
		FROM daily_site_stats WHERE site = {site:String}
		GROUP BY day ORDER BY day`,
		clickhouse.Named("site", "example.org"))
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var day time.Time
		var views, users uint64
		if err := rows.Scan(&day, &views, &users); err != nil {
			log.Fatal(err)
		}
		fmt.Println(day.Format("2006-01-02"), views, users)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}
```

`database/sql` (`clickhouse.OpenDB`) works the same way.

## Next steps

- [SQL support](sql.md): what the server accepts and where it deviates
  from ClickHouse.
- [Consistency model](consistency.md): what a client sees in its own
  region and across regions.
- [Deployment](deployment.md): the local stack, AWS serverless, ECS
  Fargate and self-managed containers.
