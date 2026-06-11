namespace Middenly.Outbox.Configuration;

public sealed class TopicOptions
{
    public bool? Ordered { get; set; }
    public int? MaxAttempts { get; set; }
}
