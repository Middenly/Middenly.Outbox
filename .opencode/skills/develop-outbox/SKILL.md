---
name: develop-outbox
description: Development guide for Middenly.Outbox. Use when adding features, fixing bugs, or working on any code in this repository. Provides project architecture, file locations, patterns, and the development checklist from AGENTS.md.
---

# Middenly.Outbox Development Guide

This skill provides guidance for developing the Middenly.Outbox NuGet package.

## Project Architecture

```
Middenly.Outbox/
├── src/                              # Main NuGet package (Middenly.Outbox)
│   ├── Abstractions/                 # Interfaces and models
│   │   ├── IOutbox.cs               # Main API: PublishAsync()
│   │   ├── IOutboxStore.cs          # Persistence abstraction
│   │   ├── IOutboxProducer.cs       # Kafka producer abstraction
│   │   ├── IOutboxSerializer.cs     # Serialization abstraction
│   │   ├── OutboxMessage.cs         # Message model (Destination, Body, Key, Headers)
│   │   ├── OutboxMessageStatus.cs   # Pending/InProgress/Completed/Failed/DeadLettered
│   │   └── PublishOptions.cs        # Fluent: WithKey(), WithHeader(), WithPartition()
│   ├── Configuration/
│   │   ├── OutboxOptions.cs         # BatchSize, PollingInterval, MaxAttempts, Topics
│   │   ├── KafkaOutboxOptions.cs    # BootstrapServers, Acks, ConfigureProducer
│   │   └── TopicOptions.cs          # Per-topic: Ordered, MaxAttempts
│   ├── Dispatcher/
│   │   └── OutboxDispatcher.cs      # BackgroundService: polls, dispatches, recovers
│   ├── Extensions/
│   │   ├── OutboxBuilder.cs         # Fluent builder: UsePostgresStore, UseKafkaProducer, Topic()
│   │   └── ServiceCollectionExtensions.cs  # AddOutbox()
│   ├── Implementation/
│   │   └── DefaultOutbox.cs         # IOutbox → IOutboxStore + notify dispatcher
│   ├── Kafka/
│   │   └── KafkaOutboxProducer.cs   # IOutboxProducer → Confluent.Kafka IProducer
│   ├── Postgres/
│   │   └── PostgresOutboxStore.cs   # IOutboxStore → Npgsql (raw SQL)
│   └── Serialization/
│       └── SystemTextJsonOutboxSerializer.cs  # Default IOutboxSerializer
│
├── src.EFCore/                       # EF Core integration (Middenly.Outbox.EntityFrameworkCore)
│   ├── Implementation/
│   │   ├── EfCoreOutbox.cs          # IOutbox → queues until SaveChanges
│   │   └── OutboxMessageCollector.cs # AsyncLocal pending messages
│   ├── Interceptors/
│   │   └── OutboxSaveChangesInterceptor.cs  # Flushes to outbox table on SaveChanges
│   └── Extensions/
│       └── EfCoreOutboxServiceCollectionExtensions.cs  # UseEfCoreOutbox<T>()
│
├── tests/Middenly.Outbox.Tests/     # All tests
│   ├── Unit/                        # Pure logic tests (no Docker)
│   └── Integration/                 # Testcontainers (PostgreSQL + Kafka)
│
└── docs/                            # VitePress documentation (source)
    ├── .vitepress/
    └── outbox/
```

## Key Design Decisions

1. **Single `IOutbox` interface** — serializer resolved internally, user never passes it
2. **Transport-agnostic naming** — `destination` not `topic`, `body` not `value`
3. **No `IOutboxProducerResolver`** — single producer per service, custom via DI
4. **`PublishOptions` fluent API** — `WithKey()`, `WithHeader()`, `WithPartition()` via `Action<PublishOptions>?`
5. **Per-topic config via `OutboxOptions.Topics`** dictionary — `Ordered`, `MaxAttempts`
6. **`AsyncLocal<OutboxMessageCollector>`** — EF Core integration uses async-local storage for scope isolation

## Common Development Tasks

### Adding a new configuration option

1. Add property to `OutboxOptions` or `KafkaOutboxOptions`
2. Use it in `OutboxDispatcher` or `KafkaOutboxProducer`
3. Update `docs/outbox/guide/configuration.md`
4. Add unit test in `tests/Unit/ConfigurationTests.cs`

### Adding a new outbox store (e.g., SQL Server)

1. Create `src/SqlServer/SqlServerOutboxStore.cs` implementing `IOutboxStore`
2. Add `UseSqlServerStore()` to `OutboxBuilder`
3. Add NuGet reference to `Microsoft.Data.SqlClient`
4. Create integration tests with Testcontainers
5. Create `docs/outbox/guide/sqlserver.md`

### Adding a new feature to the dispatcher

1. Modify `OutboxDispatcher.ExecuteAsync()` or add new method
2. If it needs configuration, add to `OutboxOptions`
3. Test with integration tests (containers required)
4. Update docs

## Development Checklist (from AGENTS.md)

After EVERY code change:

1. **Build**: `dotnet build --configuration Release` (0 errors, 0 warnings)
2. **Test**: `dotnet test --configuration Release --verbosity normal` (all pass)
3. **Coverage**: If new logic has no tests — write them
4. **Docs**: If public API changed — update `docs/outbox/guide/`
5. **Docs build**: `cd docs && npm install && npm run build`

## Test Rules

- Never skip or delete a test to make build pass
- Integration tests use Testcontainers (PostgreSQL + Kafka) — require Docker
- Unit tests must run without Docker
- Test naming: `{ClassUnderTest}Tests`, method: `{Method}_{Scenario}_{ExpectedResult}`

## Important Files

| File | Purpose |
|------|---------|
| `AGENTS.md` (root) | Development rules — always follow |
| `src/Abstractions/IOutbox.cs` | Main public API |
| `src/Dispatcher/OutboxDispatcher.cs` | Core background worker |
| `src/Postgres/PostgresOutboxStore.cs` | Database operations |
| `src/Extensions/OutboxBuilder.cs` | Fluent DI configuration |
| `docs/outbox/guide/configuration.md` | Configuration reference |
