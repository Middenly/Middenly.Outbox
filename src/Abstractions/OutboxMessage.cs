namespace Middenly.Outbox.Abstractions;

public sealed class OutboxMessage
{
    public Guid Id { get; init; } = Guid.NewGuid();
    public required string Destination { get; init; }
    public byte[]? Key { get; init; }
    public required byte[] Body { get; init; }
    public Dictionary<string, byte[]>? Headers { get; init; }
    public DateTimeOffset CreatedAt { get; init; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? DeliverAfter { get; init; }
    public int Attempts { get; set; }
    public OutboxMessageStatus Status { get; set; } = OutboxMessageStatus.Pending;
    public string? LastError { get; set; }
    public int? Partition { get; init; }
}
