namespace Middenly.Outbox.Abstractions;

public interface IOutboxSerializer
{
    byte[] Serialize<T>(T value);

    T? Deserialize<T>(byte[] data);

    object? Deserialize(byte[] data, Type type);
}
