using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaConsumer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine($"Started consumer, Ctrl-C to stop consuming");

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            Consume(cts.Token);
        }

        private static void Consume(CancellationToken token)
        {
            var topicName = "topic-demo";

            const int commitPeriod = 5;

            var cConfig = new ConsumerConfig
            {
                BootstrapServers = "",
                SaslMechanism = SaslMechanism.Plain,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslUsername = "",
                SaslPassword = "",
                GroupId = "KafkaConsumer",
                EnableAutoCommit = false,
                EnableAutoOffsetStore = false,
                AutoOffsetReset = AutoOffsetReset.Earliest,                
            };

            using (var consumer = new ConsumerBuilder<string, string>(cConfig).Build())
            {
                consumer.Subscribe(topicName);

                try
                {
                    while (true)
                    {
                        try
                        {
                            var consumeResult = consumer.Consume(token);
                            
                            Console.WriteLine($"consumed: '{consumeResult.Key}' - '{consumeResult.Value}' - '{consumeResult.TopicPartition}' - '{consumeResult.TopicPartitionOffset}'");

                            //if (consumeResult.Offset % commitPeriod == 0)
                            if(true)
                            {
                                // The Commit method sends a "commit offsets" request to the Kafka
                                // cluster and synchronously waits for the response. This is very
                                // slow compared to the rate at which the consumer is capable of
                                // consuming messages. A high performance application will typically
                                // commit offsets relatively infrequently and be designed handle
                                // duplicate messages in the event of failure.
                                try
                                {
                                    consumer.Commit(consumeResult);
                                    Console.WriteLine($"commited: {consumeResult.TopicPartitionOffset}");
                                }
                                catch (KafkaException e)
                                {
                                    Console.WriteLine($"Commit error: {e.Error.Reason}");
                                }
                            }
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"consume error: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("Closing consumer.");
                }
                finally
                {                    
                    consumer.Close();
                }
            }
        }
    }
}
