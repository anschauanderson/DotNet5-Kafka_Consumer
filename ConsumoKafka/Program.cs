using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
                SessionTimeoutMs = 10000
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

                            Console.WriteLine(
                                $"Mensagem lida: {cr.Message.Value}");

                            var pacote = JsonSerializer.Deserialize<RastreadorEvent>(cr.Message.Value);

                            Thread.Sleep(random.Next(5, 50));
                            string nomeArquivo = $"rastreadores\\{pacote.Rastreador}.txt";
                            var escreveu = EscreverArquivo(nomeArquivo, pacote.Contador);

                            try
                            {
                                consumer.Commit(cr);
                            }
                            catch (KafkaException e)
                            {
                                //Implementar Rollback se der erro no commit do offset
                                if (escreveu)
                                    ApagarUltimaLinhaArquivo(nomeArquivo);
                            }
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
        static bool EscreverArquivo(string nomeArquivo, int contador)
        {
            if (!Directory.Exists("rastreadores"))
                Directory.CreateDirectory("rastreadores"); ;

            if (!File.Exists(nomeArquivo))
                using (File.Create(nomeArquivo)) { };

            using (StreamWriter writer = new StreamWriter(nomeArquivo, true))
            {
                writer.WriteLine($"{DateTime.Now.ToString("dd/MM/yy HH:mm:ss")} - Contador {contador}");
            }

            return true;
        }

        static void ApagarUltimaLinhaArquivo(string nomeArquivo)
        {
            var lines = new List<string>();

            using (StreamReader reader = new StreamReader(nomeArquivo))
            {
                var line = reader.ReadLine();

                while (line != null && line != "")
                {
                    lines.Add(line);
                    line = reader.ReadLine();
                }
            }

            File.WriteAllText(nomeArquivo, "");

            var allExceptLast = lines.Take(lines.Count() - 1);

            using (StreamWriter writer = new StreamWriter(nomeArquivo, true))
            {
                foreach (var line in allExceptLast)
                {
                    writer.WriteLine(line);
                }
            }
        }
    }
}