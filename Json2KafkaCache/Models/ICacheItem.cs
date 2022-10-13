namespace Json2KafkaCache.Models
{
    public interface ICacheItem<out T>
    {
        T GetKey();
    }
}
