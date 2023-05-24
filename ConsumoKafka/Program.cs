using Confluent.Kafka;
using System;
using System.IO;
using System.Text.Json;
using System.Threading;

namespace ConsumoKafka
{
    class Program
    {
        static void Main(string[] args)
        {
            string bootstrapServers = "localhost:9092";
            string topicName = "topic-test";

            var config = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = "teste",
                AutoOffsetReset = AutoOffsetReset.Latest
            };

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            try
            {
                using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
                {
                    consumer.Subscribe(topicName);

                    try
                    {
                        Random random = new Random();

                        while (true)
                        {
                            var cr = consumer.Consume(cts.Token);                            
                            var pacote = JsonSerializer.Deserialize<RastreadorEvent>(cr.Message.Value);
                            
                            Thread.Sleep(random.Next(5, 500));
                            EscreverArquivo($"{pacote.Rastreador}.txt", pacote.Contador);

                            Console.WriteLine(
                                $"Mensagem lida: {cr.Message.Value}");
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        consumer.Close();
                        Console.WriteLine("Cancelada a execução do Consumer...");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exceção: {ex.GetType().FullName} | " +
                             $"Mensagem: {ex.Message}");
            }
        }
        static void EscreverArquivo(string nomeArquivo, int contador)
        {
            if (!Directory.Exists("rastreadores"))
                Directory.CreateDirectory("rastreadores"); ;

            nomeArquivo = $"rastreadores\\{nomeArquivo}";
            if (!File.Exists(nomeArquivo))
                using (File.Create(nomeArquivo)) { };

            using (StreamWriter writer = new StreamWriter(nomeArquivo, true))
            {
                writer.WriteLine($"{DateTime.Now.ToString("dd/MM/yy HH:mm:ss")} - Contador {contador}");
            }
        }
    }
}