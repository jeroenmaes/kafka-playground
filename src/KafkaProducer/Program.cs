using Confluent.Kafka;
using KafkaCommon;
using System;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace KafkaProducer
{
    public class Program
    {
        private static readonly HttpClient HttpClient = new HttpClient();
        public static async Task<TrafficLengthDto> GetTrafficLengthAsync()
        {
            var jsonResult = await HttpClient.GetStringAsync("https://map-viewer-touring-mobilis.be-mobile.biz/service/be/trafficlength");
            var result = JsonConvert.DeserializeObject<TrafficLengthDto>(jsonResult);
            result.dateRetrieved = DateTime.UtcNow;

            return result;
        }
        public static async Task Main(string[] args)
        {
            var topicName = "topic-demo";

            var config = new ProducerConfig {
                BootstrapServers = "",
                SaslMechanism = SaslMechanism.Plain,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslUsername = "",
                SaslPassword = "",
                EnableIdempotence = true,
                SocketNagleDisable = true,
                Acks = Acks.All
            };

            // If serializers are not specified, default serializers from
            // `Confluent.Kafka.Serializers` will be automatically used where
            // available. Note: by default strings are encoded as UTF8.
            using (var p = new ProducerBuilder<string, string>(config).Build())
            {
                Console.WriteLine("\n-----------------------------------------------------------------------");
                Console.WriteLine($"Producer {p.Name} producing on topic {topicName}.");
                Console.WriteLine("-----------------------------------------------------------------------");
                Console.WriteLine("To create a kafka message with UTF-8 encoded key and value:");                
                Console.WriteLine("Ctrl-C to quit.\n");

                var cancelled = false;
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true; // prevent the process from terminating.
                    cancelled = true;
                };

                while (!cancelled)
                {                    
                    try
                    {
                        var key = Guid.NewGuid().ToString();
                        var data = await GetTrafficLengthAsync();
                        var jsonData = JsonConvert.SerializeObject(data);
                        
                        var headers = new Headers();
                        headers.Add(new Header("messageType", Encoding.UTF8.GetBytes(data.GetType().ToString())));

                        var dr = await p.ProduceAsync(topicName, new Message<string, string> { Key = key , Value = jsonData, Headers = headers});
                        
                        Console.WriteLine($"Delivered '{dr.Key} '- '{dr.Value}' to '{dr.TopicPartition}' - '{dr.TopicPartitionOffset}'");

                        Thread.Sleep(TimeSpan.FromMilliseconds(500));
                    }
                    catch (ProduceException<Null, string> e)
                    {
                        Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                    }
                }

                //p.Flush(TimeSpan.FromSeconds(10));
            }
        }
    }
}
