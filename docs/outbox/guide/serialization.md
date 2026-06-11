# Serialization

Middenly.Outbox uses an extensible serialization interface to convert typed messages to and from `byte[]`.

## IOutboxSerializer

```csharp
public interface IOutboxSerializer
{
    byte[] Serialize<T>(T value);
    T? Deserialize<T>(byte[] data);
    object? Deserialize(byte[] data, Type type);
}
```

The serializer is resolved internally by `IOutbox.PublishAsync<T>()` — you don't need to inject or pass it.

## Built-in: SystemTextJsonOutboxSerializer

The default serializer uses `System.Text.Json`:

```csharp
builder.Services.AddOutbox()
    .UseSerializer<SystemTextJsonOutboxSerializer>();
```

Or with custom options:

```csharp
builder.Services.AddSingleton<IOutboxSerializer>(new SystemTextJsonOutboxSerializer(
    new JsonSerializerOptions
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false,
        Converters = { new JsonStringEnumConverter() }
    }
));
```

## Custom Serializer

Implement `IOutboxSerializer` for your preferred format:

### MessagePack

```csharp
using MessagePack;

public class MessagePackOutboxSerializer : IOutboxSerializer
{
    public byte[] Serialize<T>(T value)
    {
        return MessagePackSerializer.Serialize(value);
    }

    public T? Deserialize<T>(byte[] data)
    {
        return MessagePackSerializer.Deserialize<T>(data);
    }

    public object? Deserialize(byte[] data, Type type)
    {
        return MessagePackSerializer.Deserialize(type, data);
    }
}
```

### Registration

```csharp
builder.Services.AddSingleton<IOutboxSerializer, MessagePackOutboxSerializer>();
// or
builder.Services.AddOutbox()
    .UseSerializer<MessagePackOutboxSerializer>();
```

## Usage

When using typed messages, the serializer is resolved automatically:

```csharp
public class OrderService
{
    private readonly IOutbox _outbox;

    public OrderService(IOutbox outbox)
    {
        _outbox = outbox;
    }

    public async Task CreateOrderAsync(Order order)
    {
        // The serializer is resolved internally — no need to pass it
        await _outbox.PublishAsync("orders", new OrderCreated
        {
            OrderId = order.Id,
            Total = order.Total
        });
    }
}
```

## Raw Bytes

If you don't need serialization, pass raw bytes directly:

```csharp
await _outbox.PublishAsync("topic", Encoding.UTF8.GetBytes("raw message"));
```
