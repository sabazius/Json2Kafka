using Confluent.Kafka;
using Json2KafkaCache.Models;
using Json2KafkaCache.Serializers;
using Newtonsoft.Json;

namespace Json2KafkaCache.Caches
{
    public class JsonCacheDistributor<TKey, TValue> where TValue : ICacheItem<TKey>
    {
        private readonly IProducer<TKey, TValue> _producer;
        private readonly List<TValue> _data;

        public JsonCacheDistributor(CancellationTokenSource? cts = default)
        {
            _data = new List<TValue>();

            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:9092",
            };

            _producer = new ProducerBuilder<TKey, TValue>(config)
                .SetValueSerializer(new MsgPackSerializer<TValue>())
                .Build();

            var fileName = 
                Directory.GetFiles("Data", "*.json")
                    .FirstOrDefault(x => x.Contains(typeof(TValue).Name));

            if (!string.IsNullOrEmpty(fileName))
            {
                using (StreamReader sr = new StreamReader(fileName))
                {
                    string json = sr.ReadToEnd();
                    _data.AddRange(JsonConvert.DeserializeObject<List<TValue>>(json) ?? Enumerable.Empty<TValue>());
                }
            }

            var _cts = cts ?? new CancellationTokenSource();

            if (_data.Count > 0)
            {
                Task.Run(async () =>
                {
                    await Execute(_cts.Token);
                }, _cts.Token);
            }

        }

        public async Task Execute(CancellationToken token)
        {
            foreach (var cacheItem in _data)
            {
                var message = new Message<TKey, TValue>()
                {
                    Key = cacheItem.GetKey(),
                    Value = cacheItem
                };

                await _producer.ProduceAsync($"Cache.{nameof(TValue)}",message, token);
            }
        }

    }
}
