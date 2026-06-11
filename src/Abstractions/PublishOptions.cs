namespace Middenly.Outbox.Abstractions;

public sealed class PublishOptions
{
    public byte[]? Key { get; set; }
    public int? Partition { get; set; }
    public DateTimeOffset? DeliverAfter { get; set; }
    public Dictionary<string, byte[]> Headers { get; set; } = new();

    public PublishOptions WithKey(byte[] key)
    {
        Key = key;
        return this;
    }

    public PublishOptions WithKey(string key)
    {
        Key = System.Text.Encoding.UTF8.GetBytes(key);
        return this;
    }

    public PublishOptions WithPartition(int partition)
    {
        Partition = partition;
        return this;
    }

    public PublishOptions WithHeader(string name, byte[] value)
    {
        Headers[name] = value;
        return this;
    }

    public PublishOptions WithHeader(string name, string value)
    {
        Headers[name] = System.Text.Encoding.UTF8.GetBytes(value);
        return this;
    }

    public PublishOptions DeliverAfterDelay(TimeSpan delay)
    {
        DeliverAfter = DateTimeOffset.UtcNow.Add(delay);
        return this;
    }

    public PublishOptions DeliverAt(DateTimeOffset when)
    {
        DeliverAfter = when;
        return this;
    }
}
