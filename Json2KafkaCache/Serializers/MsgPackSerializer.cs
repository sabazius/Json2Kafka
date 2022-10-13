using Confluent.Kafka;
using MessagePack;

namespace Json2KafkaCache.Serializers
{
    public class MsgPackSerializer<T> : ISerializer<T>
    {
        public byte[] Serialize(T data, SerializationContext context)
        {
            return MessagePackSerializer.Serialize(data);
        }
    }
}
