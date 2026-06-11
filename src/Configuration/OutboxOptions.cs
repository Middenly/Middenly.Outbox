namespace Middenly.Outbox.Configuration;

public sealed class OutboxOptions
{
    public int BatchSize { get; set; } = 100;
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(5);
    public int MaxAttempts { get; set; } = 5;
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(30);
    public bool EnableDeadLetter { get; set; } = true;
    public TimeSpan? CleanupInterval { get; set; }
    public TimeSpan MessageRetention { get; set; } = TimeSpan.FromDays(7);
    public TimeSpan StuckMessageTimeout { get; set; } = TimeSpan.FromMinutes(5);
    public TimeSpan RecoveryPollingInterval { get; set; } = TimeSpan.FromMinutes(1);
    public string TableName { get; set; } = "outbox_messages";
    public string SchemaName { get; set; } = "public";
    public Dictionary<string, TopicOptions> Topics { get; set; } = new(StringComparer.OrdinalIgnoreCase);
    public TopicOptions DefaultTopic { get; set; } = new();

    public TopicOptions GetTopicOptions(string destination)
    {
        if (Topics.TryGetValue(destination, out var topicOptions))
        {
            return new TopicOptions
            {
                Ordered = topicOptions.Ordered ?? DefaultTopic.Ordered,
                MaxAttempts = topicOptions.MaxAttempts ?? DefaultTopic.MaxAttempts
            };
        }

        return DefaultTopic;
    }
}
