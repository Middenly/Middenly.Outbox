using System.Text.Json;
using Middenly.Outbox.Abstractions;

namespace Middenly.Outbox.Serialization;

public sealed class SystemTextJsonOutboxSerializer : IOutboxSerializer
{
    private readonly JsonSerializerOptions _options;

    public SystemTextJsonOutboxSerializer(JsonSerializerOptions? options = null)
    {
        _options = options ?? new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };
    }

    public byte[] Serialize<T>(T value)
    {
        return JsonSerializer.SerializeToUtf8Bytes(value, _options);
    }

    public T? Deserialize<T>(byte[] data)
    {
        return JsonSerializer.Deserialize<T>(data, _options);
    }

    public object? Deserialize(byte[] data, Type type)
    {
        return JsonSerializer.Deserialize(data, type, _options);
    }
}
