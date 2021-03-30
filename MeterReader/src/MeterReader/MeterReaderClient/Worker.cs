using Grpc.Core;
using Grpc.Net.Client;
using MeterReaderWeb.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MeterReaderClient
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IConfiguration _configuration;
        private readonly ReadingFactory _readingFactory;
        private readonly ILoggerFactory _loggerFactory;
        private MeterReadingService.MeterReadingServiceClient _client = null;
        private string _token;
        private DateTime _expiration = DateTime.MinValue;

        public Worker(ILogger<Worker> logger,IConfiguration configuration,
            ReadingFactory readingFactory,ILoggerFactory loggerFactory)
        {
            _logger = logger;
            _configuration = configuration;
            _readingFactory = readingFactory;
            _loggerFactory = loggerFactory;
        }

        protected MeterReadingService.MeterReadingServiceClient Client
        {
            get
            {
                if(_client == null)
                {
                    var opt = new GrpcChannelOptions()
                    {
                        LoggerFactory = _loggerFactory
                    };

                    var channel = GrpcChannel
                        .ForAddress(_configuration.GetValue<string>("Service:ServiceUrl"),opt);

                    _client = new MeterReadingService.MeterReadingServiceClient(channel);
                }

                return _client;
            }
        }


        protected bool NeedsLogin() => string.IsNullOrWhiteSpace(_token) ||
            _expiration > DateTime.UtcNow;

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var counter = 0;

            var customerId = _configuration.GetValue<int>("Service:CustomerId");

            while (!stoppingToken.IsCancellationRequested)
            {
                counter++;

                if (counter % 10 == 0)
                {
                    Console.WriteLine("Sending Diagnostics");

                    var stream = Client.SendDiagnostics();
                    for (int i = 0; i < 5; i++)
                    {
                        var reading = await _readingFactory.Generate(customerId);
                        await stream.RequestStream.WriteAsync(reading);
                    }

                    await stream.RequestStream.CompleteAsync();
                }

                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);

                var pkt = new ReadingPacket()
                {
                    Successful = ReadingStatus.Success,
                    Notes = "This is test"
                };

                for (int i = 0; i < 5; i++)
                {
                    pkt.Readings.Add(await _readingFactory.Generate(customerId));
                }

                try
                {
                    if(!NeedsLogin() || await GenerateToken())
                    {
                        var headers = new Metadata();
                        headers.Add("Authorization", $"Bearer {_token}");

                        var result = await Client.AddReadingAsync(pkt,headers: headers);

                        if (result.Success == ReadingStatus.Success)
                        {
                            _logger.LogInformation("Sucessfully sent");
                        }
                        else
                        {
                            _logger.LogInformation("Failed to sent");
                        }
                    }
                }
                catch(RpcException ex)
                {
                    if(ex.StatusCode == StatusCode.OutOfRange)
                    {
                        _logger.LogError($"{ex.Trailers}");
                    }

                    _logger.LogError($"Exception Thrown: {ex}");
                }

                await Task.Delay(_configuration.GetValue<int>("Service:DelayInterval"), stoppingToken);
            }
        }

        private async Task<bool> GenerateToken()
        {
            var request = new TokenRequest()
            {
                Username = _configuration["Service:Username"],
                Password = _configuration["Service:Password"]
            };

            var response = await Client.CreateTokenAsync(request);

            if(response.Success)
            {
                _token = response.Token;
                _expiration = response.Expiration.ToDateTime();

                return true;
            }

            return false;
        }
    }
}
