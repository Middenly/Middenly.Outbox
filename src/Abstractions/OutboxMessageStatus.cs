namespace Middenly.Outbox.Abstractions;

public enum OutboxMessageStatus
{
    Pending = 0,
    InProgress = 1,
    Completed = 2,
    Failed = 3,
    DeadLettered = 4
}
