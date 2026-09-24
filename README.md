# Oracle HYBRID replication patch

## New classes

- `OracleHybridReplicationImpl` — alternative Oracle pipeline, `ReplicationPipeline.HYBRID`.
- `OracleHybridMetadataCopyStreamer` — flat extraction + Java streaming aggregation + final PostgreSQL COPY.
- `ReplicationPipeline` — `STANDARD` / `HYBRID`.

Existing `OracleReplicationImpl` and `OracleMetadataCopyStreamer` are intentionally left unchanged.

## Hybrid extraction phases

1. `hybrid_objects.sql` loads the logical TABLE/VIEW/MATERIALIZED_VIEW catalog.
2. Four detail workers run in parallel:
   - columns
   - constraints
   - short views (`TEXT_VC`, no LONG)
   - long views + materialized views (`LONG`)
3. Java aggregates columns/constraints per object without retaining every column DTO in heap.
4. Only after extraction succeeds does the target transaction begin:
   - DELETE current database table snapshot
   - PostgreSQL COPY final rows
   - COMMIT

## SQL files

Place all `hybrid_*.sql` files in `src/main/resources/sql/oracle/`.

## Configuration

Merge `application-oracle-hybrid.yaml.snippet` into `application.yaml`.

Default:

```yaml
replication:
  oracle:
    pipeline: HYBRID
    hybrid:
      parallelism: 4
      fetch-size: 10000
      query-timeout-seconds: 0
      allow-empty-object-snapshot: false
```

## REST override

```json
{
  "serviceName": "oracle_service",
  "async": true,
  "pipeline": "HYBRID"
}
```

Use `STANDARD` to run the existing `OracleReplicationImpl`.

## Integration files modified

- `ReplicationService` — default `getPipeline()` = STANDARD.
- `ReplicationServiceRegistry` — registry key is `(DatabaseType, ReplicationPipeline)`.
- `ReplicationRequestDto` — optional `pipeline`.
- `ReplicationController` — request override + Oracle default from YAML.
- `SqlQueryProvider` — overload accepting a query file name, used for `hybrid_*.sql`.
