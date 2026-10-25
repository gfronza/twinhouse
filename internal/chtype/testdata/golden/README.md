# chtype golden data generators

The `golden_*_data_test.go` files in `internal/chtype` are generated from a
real ClickHouse server (the reference oracle). To regenerate:

```sh
docker run -d --name ch-golden -p 18126:8123 -e CLICKHOUSE_SKIP_USER_SETUP=1 \
  --ulimit nofile=262144:262144 clickhouse/clickhouse-server:25.8-alpine
cd internal/chtype/testdata/golden
for g in gen_format gen_params gen_cast gen_values gen_sup gen_types; do python3 $g.py; done
gofmt -w ../..
docker rm -f ch-golden
```

`CH_URL` overrides the server URL. Inputs live in `*_cases.txt` /
`type_cases.py` and inline in `gen_format.py`. Intentional deviations from
ClickHouse are listed in the corresponding `golden_*_test.go` files.
