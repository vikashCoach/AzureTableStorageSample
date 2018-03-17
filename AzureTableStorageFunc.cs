using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using System.Net.Http;
using System.Text;
using System;
using System.Configuration;
using Microsoft.Azure.CosmosDB.Table;
using System.Linq;
using Microsoft.Azure.Storage;
namespace AzureTableStorageFunc.cs
{
    public static class AzureTableStorageFunc
    {
        [FunctionName("AzureTableStorageFunc")]
        public static async Task<HttpResponseMessage> Run([EventGridTrigger] EventGridEvent eventGridEvent, TraceWriter log)
        {
            log.Info($"Event {eventGridEvent.Topic} was received at {eventGridEvent.PublishTime} ");

            try
            {
                var isSaveSuccess = await StoreDataAsync(eventGridEvent,log);
                if (!isSaveSuccess)
                    throw new Exception("Error while saving data");

                log.Info($"Event {eventGridEvent.Topic} was successfully published at {DateTime.UtcNow} ");


                return new HttpResponseMessage(System.Net.HttpStatusCode.OK)
                {
                    Content = new StringContent($"Request was processed{eventGridEvent.EventTime}", Encoding.UTF8, "application/json")
                };
            }
            catch (Exception ex)
            {
                return new HttpResponseMessage(System.Net.HttpStatusCode.InternalServerError)
                {
                    Content = new StringContent($"{ex?.Message}", Encoding.UTF8, "application/json")
                };
            }
        }

        private static async Task<bool> StoreDataAsync(EventGridEvent eventGridEvent,TraceWriter traceWriter)
        {
            try
            {
                var eventData = eventGridEvent.Data.ToObject<Data>(); //Blob Info
                var connectionString = ConfigurationManager.AppSettings["YourConnectionStringForStorageAccount"]?.ToString() ?? throw new ArgumentNullException($"Connection string for table storage was missing at {DateTime.UtcNow}");

                var storageAccount = CloudStorageAccount.Parse(connectionString);

                var tableClient = storageAccount.CreateCloudTableClient();
                var table = tableClient.GetTableReference("TableName");

                await table.CreateIfNotExistsAsync(); //Create a table storage if not exists
                
                var storageAccountName = eventGridEvent.Topic.Split('/').Last();
                var rawConsumptionData = new RawEventData(storageAccountName, eventGridEvent.Id)
                {
                    Topic = eventGridEvent.Topic,
                    Subject = eventGridEvent.Subject,
                    EventType = eventGridEvent.EventType,
                    EventTime = eventGridEvent.EventTime,
                    CustomerHexId = eventGridEvent.Topic,//Fetch Customer HEx from Account Name--To Do
                    Api = eventData.Api,
                    ClientRequestId = eventData.ClientRequestId,
                    ContentType = eventData.ContentType,
                    ContentLength = eventData.ContentLength,
                    ETag = eventData.Etag,
                    BlobType = eventData.BlobType,
                    Url = eventData.Url
                };

                var insertOperation = TableOperation.Insert(rawConsumptionData);
                await table.ExecuteAsync(insertOperation);

                return true;
            }
            catch (Exception ex)
            {
                traceWriter.Error(ex?.Message, ex);
                return await Task.FromResult(false);
            }
        }
    }
    public class Data
    {
        public string Api { get; set; }
        public string ClientRequestId { get; set; }
        public string RequestId { get; set; }
        public string Etag { get; set; }
        public string ContentType { get; set; }
        public int ContentLength { get; set; }
        public string BlobType { get; set; }
        public string Url { get; set; }
        public string Sequencer { get; set; }
        public string StorageDiagnostics { get; set; }

    }
    public class RawEventData : TableEntity
    {
        public RawEventData() { }
        public RawEventData(string storageAccountName, string Id)
        {
            PartitionKey = storageAccountName;
            RowKey = Id; //primaryKey
        }
        public string Topic { get; set; }
        public string Subject { get; set; }
        public string EventType { get; set; }
        public DateTime EventTime { get; set; }
        public string CustomerHexId { get; set; }
        public string Api { get; set; }
        public string ClientRequestId { get; set; }
        public string RequestId { get; set; }
        public string Etag { get; set; }
        public string ContentType { get; set; }
        public int ContentLength { get; set; }
        public string BlobType { get; set; }
        public string Url { get; set; }
    }
}




