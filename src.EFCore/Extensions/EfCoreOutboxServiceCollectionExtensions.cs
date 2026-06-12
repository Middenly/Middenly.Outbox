using Middenly.Outbox.Abstractions;
using Middenly.Outbox.EntityFrameworkCore.Implementation;
using Middenly.Outbox.EntityFrameworkCore.Interceptors;
using Middenly.Outbox.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Middenly.Outbox.EntityFrameworkCore.Extensions;

public static class EfCoreOutboxServiceCollectionExtensions
{
    public static OutboxBuilder UseEfCoreOutbox<TDbContext>(this OutboxBuilder builder)
        where TDbContext : DbContext
    {
        builder.Services.TryAddScoped<EfCoreOutbox>();
        builder.Services.TryAddScoped<OutboxSaveChangesInterceptor>();

        // Replace IOutbox: singleton → scoped (EfCoreOutbox)
        var existing = builder.Services.FirstOrDefault(d => d.ServiceType == typeof(IOutbox));
        if (existing is not null) builder.Services.Remove(existing);
        builder.Services.AddScoped<IOutbox>(sp => sp.GetRequiredService<EfCoreOutbox>());

        return builder;
    }

    public static DbContextOptionsBuilder UseOutboxInterceptor(
        this DbContextOptionsBuilder optionsBuilder,
        IServiceProvider serviceProvider)
    {
        var interceptor = serviceProvider.GetRequiredService<OutboxSaveChangesInterceptor>();
        optionsBuilder.AddInterceptors(interceptor);
        return optionsBuilder;
    }
}
