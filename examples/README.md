# Clavis Examples

These examples are small and focused on the three strongest Clavis use cases:

- `postgres-job-runner`: a singleton scheduled job with fenced downstream writes
- `controller-leader`: active/passive controller leadership
- `tenant-reconciler`: per-tenant serialization with lock names

Each example uses the public Go SDK in `pkg/client` and expects a running Clavis cluster.

## Common environment variables

- `CLAVIS_SEEDS`: comma-separated gRPC seed addresses, for example `127.0.0.1:9000,127.0.0.1:9001,127.0.0.1:9002`
- `CLAVIS_OWNER_ID`: optional owner identity; if unset the examples derive one from hostname and PID

## Running an example

```bash
CLAVIS_SEEDS=127.0.0.1:9000 go run ./examples/controller-leader
```

The examples are templates for real applications. They are intentionally dependency-light, so the fenced database example uses an in-memory store while documenting the Postgres compare-and-set shape you would use in production.
