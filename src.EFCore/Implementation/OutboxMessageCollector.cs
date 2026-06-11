using System.Collections.Concurrent;
using Middenly.Outbox.Abstractions;

namespace Middenly.Outbox.EntityFrameworkCore.Implementation;

public sealed class OutboxMessageCollector
{
    private static readonly AsyncLocal<OutboxMessageCollector?> _current = new();

    private readonly ConcurrentBag<OutboxMessage> _pending = new();

    public IReadOnlyCollection<OutboxMessage> Pending => _pending;

    public static OutboxMessageCollector Current =>
        _current.Value ??= new OutboxMessageCollector();

    public static void Reset() => _current.Value = null;

    public void Add(OutboxMessage message)
    {
        _pending.Add(message);
    }

    public IReadOnlyList<OutboxMessage> Drain()
    {
        var messages = _pending.ToArray();
        _pending.Clear();
        return messages;
    }
}
