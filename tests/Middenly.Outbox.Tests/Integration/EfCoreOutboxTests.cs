using System.Text;
using Confluent.Kafka;
using Middenly.Outbox.Abstractions;
using Middenly.Outbox.Configuration;
using Middenly.Outbox.EntityFrameworkCore.Extensions;
using Middenly.Outbox.EntityFrameworkCore.Implementation;
using Middenly.Outbox.Extensions;
using FluentAssertions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Testcontainers.Kafka;
using Testcontainers.PostgreSql;
using Xunit;

namespace Middenly.Outbox.Tests.Integration;

public class EfCoreOutboxTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .WithDatabase("outbox_test")
        .WithUsername("test")
        .WithPassword("test")
        .Build();

    private readonly KafkaContainer _kafka = new KafkaBuilder()
        .WithImage("confluentinc/cp-kafka:7.6.0")
        .Build();

    public async Task InitializeAsync()
    {
        await _postgres.StartAsync();
        await _kafka.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _kafka.DisposeAsync();
        await _postgres.DisposeAsync();
    }

    [Fact]
    public async Task SaveChanges_WithOutboxMessage_ShouldStoreInSameTransaction()
    {
        // Arrange
        var connectionString = _postgres.GetConnectionString();
        var bootstrapServers = _kafka.GetBootstrapAddress();

        var services = new ServiceCollection();
        services.AddLogging(b => b.AddConsole());

        services.AddOutbox(options =>
        {
            options.PollingInterval = TimeSpan.FromSeconds(1);
            options.MaxAttempts = 3;
        })
        .UsePostgresStore(connectionString)
        .UseKafkaProducer(kafka =>
        {
            kafka.BootstrapServers = bootstrapServers;
            kafka.Acks = Acks.All;
        })
        .UseEfCoreOutbox<TestDbContext>();

        services.AddDbContext<TestDbContext>((sp, options) =>
        {
            options.UseNpgsql(connectionString);
            options.UseOutboxInterceptor(sp);
        });

        await using var provider = services.BuildServiceProvider();

        var store = provider.GetRequiredService<IOutboxStore>();
        await store.InitializeAsync();

        var hostedServices = provider.GetServices<IHostedService>().ToList();
        foreach (var svc in hostedServices) await svc.StartAsync(CancellationToken.None);

        try
        {
            // Create test table directly
            await using (var conn = new Npgsql.NpgsqlConnection(connectionString))
            {
                await conn.OpenAsync();
                await using var cmd = new Npgsql.NpgsqlCommand(
                    "CREATE TABLE IF NOT EXISTS test_entities (\"Id\" SERIAL PRIMARY KEY, \"Name\" VARCHAR(200), \"Amount\" NUMERIC)",
                    conn);
                await cmd.ExecuteNonQueryAsync();
            }

            // Act
            using (var scope = provider.CreateScope())
            {
                var context = scope.ServiceProvider.GetRequiredService<TestDbContext>();

                var entity = new TestEntity { Name = "Test Order", Amount = 42.50m };
                context.Entities.Add(entity);

                var outbox = scope.ServiceProvider.GetRequiredService<IOutbox>();
                await outbox.PublishAsync("ef-test-topic", Encoding.UTF8.GetBytes("Order Test Order"));

                await context.SaveChangesAsync();
            }

            await Task.Delay(TimeSpan.FromSeconds(8));

            // Assert
            using var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = $"test-{Guid.NewGuid():N}",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                AllowAutoCreateTopics = true
            }).Build();

            consumer.Subscribe("ef-test-topic");
            var result = consumer.Consume(TimeSpan.FromSeconds(10));
            result.Should().NotBeNull();
            Encoding.UTF8.GetString(result.Message.Value).Should().Contain("Test Order");
        }
        finally
        {
            foreach (var svc in hostedServices) await svc.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task SaveChanges_MultipleMessages_ShouldStoreAllInTransaction()
    {
        // Arrange
        var connectionString = _postgres.GetConnectionString();
        var bootstrapServers = _kafka.GetBootstrapAddress();

        var services = new ServiceCollection();
        services.AddLogging(b => b.AddConsole());

        services.AddOutbox(options =>
        {
            options.PollingInterval = TimeSpan.FromSeconds(1);
            options.MaxAttempts = 3;
        })
        .UsePostgresStore(connectionString)
        .UseKafkaProducer(kafka => kafka.BootstrapServers = bootstrapServers)
        .UseEfCoreOutbox<TestDbContext>();

        services.AddDbContext<TestDbContext>((sp, options) =>
        {
            options.UseNpgsql(connectionString);
            options.UseOutboxInterceptor(sp);
        });

        await using var provider = services.BuildServiceProvider();

        var store = provider.GetRequiredService<IOutboxStore>();
        await store.InitializeAsync();

        var hostedServices = provider.GetServices<IHostedService>().ToList();
        foreach (var svc in hostedServices) await svc.StartAsync(CancellationToken.None);

        try
        {
            // Create test table directly
            await using (var conn = new Npgsql.NpgsqlConnection(connectionString))
            {
                await conn.OpenAsync();
                await using var cmd = new Npgsql.NpgsqlCommand(
                    "CREATE TABLE IF NOT EXISTS test_entities (\"Id\" SERIAL PRIMARY KEY, \"Name\" VARCHAR(200), \"Amount\" NUMERIC)",
                    conn);
                await cmd.ExecuteNonQueryAsync();
            }

            // Act - multiple outbox messages in one SaveChanges
            using (var scope = provider.CreateScope())
            {
                var context = scope.ServiceProvider.GetRequiredService<TestDbContext>();

                var outbox = scope.ServiceProvider.GetRequiredService<IOutbox>();

                for (int i = 0; i < 5; i++)
                {
                    await outbox.PublishAsync("ef-multi-topic", Encoding.UTF8.GetBytes($"msg-{i}"));
                }

                await context.SaveChangesAsync();
            }

            await Task.Delay(TimeSpan.FromSeconds(8));

            // Assert
            using var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = $"test-{Guid.NewGuid():N}",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                AllowAutoCreateTopics = true
            }).Build();

            consumer.Subscribe("ef-multi-topic");

            var consumed = new List<string>();
            for (int i = 0; i < 5; i++)
            {
                var result = consumer.Consume(TimeSpan.FromSeconds(15));
                if (result != null) consumed.Add(Encoding.UTF8.GetString(result.Message.Value));
            }

            consumed.Should().HaveCount(5);
        }
        finally
        {
            foreach (var svc in hostedServices) await svc.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Collector_ShouldResetAfterDrain()
    {
        // Arrange
        var connectionString = _postgres.GetConnectionString();

        var services = new ServiceCollection();
        services.AddLogging(b => b.AddConsole());

        services.AddOutbox()
            .UsePostgresStore(connectionString)
            .UseKafkaProducer(kafka => kafka.BootstrapServers = "localhost:9092")
            .UseEfCoreOutbox<TestDbContext>();

        services.AddDbContext<TestDbContext>((sp, options) =>
        {
            options.UseNpgsql(connectionString);
            options.UseOutboxInterceptor(sp);
        });

        await using var provider = services.BuildServiceProvider();

        // Act - first scope: publish and drain
        using (var scope1 = provider.CreateScope())
        {
            var outbox1 = scope1.ServiceProvider.GetRequiredService<IOutbox>();
            await outbox1.PublishAsync("topic1", Encoding.UTF8.GetBytes("msg1"));
            OutboxMessageCollector.Current.Pending.Should().HaveCount(1);
            OutboxMessageCollector.Current.Drain();
            OutboxMessageCollector.Reset();
        }

        // Second scope: should start fresh
        using (var scope2 = provider.CreateScope())
        {
            var outbox2 = scope2.ServiceProvider.GetRequiredService<IOutbox>();
            await outbox2.PublishAsync("topic2", Encoding.UTF8.GetBytes("msg2"));

            // Assert - new messages should be independent
            OutboxMessageCollector.Current.Pending.Should().HaveCount(1);
        }
    }

    [Fact]
    public async Task EfCoreOutbox_PublishAsync_ShouldQueueWithoutDbWrite()
    {
        // Arrange
        var connectionString = _postgres.GetConnectionString();

        var services = new ServiceCollection();
        services.AddLogging(b => b.AddConsole());

        services.AddOutbox()
            .UsePostgresStore(connectionString)
            .UseKafkaProducer(kafka => kafka.BootstrapServers = "localhost:9092")
            .UseEfCoreOutbox<TestDbContext>();

        services.AddDbContext<TestDbContext>((sp, options) =>
        {
            options.UseNpgsql(connectionString);
            options.UseOutboxInterceptor(sp);
        });

        await using var provider = services.BuildServiceProvider();

        var store = provider.GetRequiredService<IOutboxStore>();
        await store.InitializeAsync();

        // Act - publish without SaveChanges
        using (var scope = provider.CreateScope())
        {
            var outbox = scope.ServiceProvider.GetRequiredService<IOutbox>();
            await outbox.PublishAsync("test-topic", Encoding.UTF8.GetBytes("queued"));
        }

        // Assert - nothing in DB
        var pending = await store.GetPendingAsync(100);
        pending.Should().BeEmpty();
    }

    [Fact]
    public async Task SaveChanges_WhenBusinessSaveFails_ShouldRollbackOutboxInsert()
    {
        // Arrange
        var connectionString = _postgres.GetConnectionString();
        var services = CreateEfCoreServices(connectionString);

        await using var provider = services.BuildServiceProvider();

        var store = provider.GetRequiredService<IOutboxStore>();
        await store.InitializeAsync();
        await RecreateTestEntitiesTableAsync(connectionString, nameRequired: true);

        // Act
        using (var scope = provider.CreateScope())
        {
            var context = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            context.Entities.Add(new TestEntity { Name = null!, Amount = 10m });

            var outbox = scope.ServiceProvider.GetRequiredService<IOutbox>();
            await outbox.PublishAsync("rollback-topic", Encoding.UTF8.GetBytes("should-rollback"));

            var act = () => context.SaveChangesAsync();
            await act.Should().ThrowAsync<Exception>();
        }

        // Assert
        var outboxCount = await CountRowsAsync(connectionString, "outbox_messages");
        var entityCount = await CountRowsAsync(connectionString, "test_entities");
        outboxCount.Should().Be(0);
        entityCount.Should().Be(0);
    }

    [Fact]
    public async Task SaveChanges_WhenOutboxInsertFails_ShouldNotSaveBusinessChanges()
    {
        // Arrange
        var connectionString = _postgres.GetConnectionString();
        var services = CreateEfCoreServices(connectionString, options =>
        {
            options.SchemaName = "missing_schema";
            options.TableName = "missing_outbox";
        });

        await using var provider = services.BuildServiceProvider();
        await RecreateTestEntitiesTableAsync(connectionString, nameRequired: true);

        // Act
        using (var scope = provider.CreateScope())
        {
            var context = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            context.Entities.Add(new TestEntity { Name = "should not save", Amount = 10m });

            var outbox = scope.ServiceProvider.GetRequiredService<IOutbox>();
            await outbox.PublishAsync("missing-outbox-topic", Encoding.UTF8.GetBytes("should-fail"));

            var act = () => context.SaveChangesAsync();
            await act.Should().ThrowAsync<Exception>();
        }

        // Assert
        var entityCount = await CountRowsAsync(connectionString, "test_entities");
        entityCount.Should().Be(0);
    }

    [Fact]
    public async Task SaveChanges_WithExistingTransaction_ShouldLeaveCommitToCaller()
    {
        // Arrange
        var connectionString = _postgres.GetConnectionString();
        var services = CreateEfCoreServices(connectionString);

        await using var provider = services.BuildServiceProvider();

        var store = provider.GetRequiredService<IOutboxStore>();
        await store.InitializeAsync();
        await RecreateTestEntitiesTableAsync(connectionString, nameRequired: true);

        await using var scope = provider.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<TestDbContext>();
        await using var transaction = await context.Database.BeginTransactionAsync();

        context.Entities.Add(new TestEntity { Name = "committed by caller", Amount = 42m });
        var outbox = scope.ServiceProvider.GetRequiredService<IOutbox>();
        await outbox.PublishAsync("caller-transaction-topic", Encoding.UTF8.GetBytes("transactional"));

        // Act
        await context.SaveChangesAsync();

        // Assert before caller commits
        (await CountRowsAsync(connectionString, "outbox_messages")).Should().Be(0);
        (await CountRowsAsync(connectionString, "test_entities")).Should().Be(0);

        await transaction.CommitAsync();

        // Assert after caller commits
        (await CountRowsAsync(connectionString, "outbox_messages")).Should().Be(1);
        (await CountRowsAsync(connectionString, "test_entities")).Should().Be(1);
    }

    private static ServiceCollection CreateEfCoreServices(
        string connectionString,
        Action<OutboxOptions>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.AddConsole());

        services.AddOutbox(configure)
            .UsePostgresStore(connectionString)
            .UseKafkaProducer(kafka => kafka.BootstrapServers = "localhost:9092")
            .UseEfCoreOutbox<TestDbContext>();

        services.AddDbContext<TestDbContext>((sp, options) =>
        {
            options.UseNpgsql(connectionString);
            options.UseOutboxInterceptor(sp);
        });

        return services;
    }

    private static async Task RecreateTestEntitiesTableAsync(string connectionString, bool nameRequired)
    {
        await using var conn = new Npgsql.NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        await using (var drop = new Npgsql.NpgsqlCommand("DROP TABLE IF EXISTS test_entities", conn))
        {
            await drop.ExecuteNonQueryAsync();
        }

        var nameColumn = nameRequired ? "\"Name\" VARCHAR(200) NOT NULL" : "\"Name\" VARCHAR(200)";
        await using var create = new Npgsql.NpgsqlCommand(
            $"CREATE TABLE test_entities (\"Id\" SERIAL PRIMARY KEY, {nameColumn}, \"Amount\" NUMERIC)",
            conn);
        await create.ExecuteNonQueryAsync();
    }

    private static async Task<int> CountRowsAsync(string connectionString, string tableName)
    {
        await using var conn = new Npgsql.NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        await using var cmd = new Npgsql.NpgsqlCommand($"SELECT COUNT(*) FROM {tableName}", conn);
        return Convert.ToInt32(await cmd.ExecuteScalarAsync());
    }
}

public class TestEntity
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public decimal Amount { get; set; }
}

public class TestDbContext : DbContext
{
    public DbSet<TestEntity> Entities => Set<TestEntity>();

    public TestDbContext(DbContextOptions<TestDbContext> options) : base(options) { }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<TestEntity>().ToTable("test_entities");
    }
}
