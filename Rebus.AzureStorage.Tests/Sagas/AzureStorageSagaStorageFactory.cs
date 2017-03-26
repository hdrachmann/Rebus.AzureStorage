using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Blob.Protocol;
using Microsoft.WindowsAzure.Storage.Table;
using Rebus.AzureStorage.Sagas;
using Rebus.Logging;
using Rebus.Sagas;
using Rebus.Tests.Contracts.Sagas;

namespace Rebus.AzureStorage.Tests.Sagas
{
    public class AzureStorageSagaStorageFactory : AzureStorageFactoryBase, ISagaStorageFactory//, ISagaSnapshotStorageFactory
    {
        //public static readonly string ContainerName = "newrebussagastoragetestcontainer";
        //public static readonly string TableName = "newrebussagastoragetesttable";

        static int _nameIndex = 1;

        string _containerName;
        string _tableName;

        public ISagaStorage GetSagaStorage()
        {
            var index = _nameIndex++;

            _containerName = $"newrebussagastoragetestcontainer{index}";
            _tableName = $"newrebussagastoragetesttable{index}";

            var storage = new AzureStorageSagaStorage(StorageAccount, new ConsoleLoggerFactory(false), _tableName, _containerName);

            storage.Initialize();

            return storage;
        }

        public void CleanUp()
        {
            var blobClient = StorageAccount.CreateCloudBlobClient();
            var tableClient = StorageAccount.CreateCloudTableClient();

            //Task.WaitAll(ClearContainer(blobClient, ContainerName), ClearTable(tableClient, TableName));

            //return;

            Task.WaitAll(ClearBlobs(blobClient), ClearTables(tableClient));
        }

        static async Task ClearTables(CloudTableClient tableClient)
        {

            TableContinuationToken token = null;
            do
            {
                var segement = await tableClient.ListTablesSegmentedAsync(token);

                token = segement.ContinuationToken;
                await Task.WhenAll(
                        segement.Results
                            .Select(async table => await ClearTable(tableClient, table.Name))
                    )
                    ;
            } while (token != null);
        }

        static async Task ClearBlobs(CloudBlobClient blobClient)
        {
            BlobContinuationToken token = null;
            do
            {
                var segment = await blobClient.ListContainersSegmentedAsync(token);
                token = segment.ContinuationToken;

                await Task.WhenAll(segment.Results
                    .Select(async container => await ClearContainer(blobClient, container.Name))
                );

            } while (token != null);



        }

        static async Task ClearContainer(CloudBlobClient blobClient, string containerName)
        {
            var containerReference = blobClient.GetContainerReference(containerName);

            if (!await containerReference.ExistsAsync()) return;

            Console.WriteLine($"Clearing container '{containerReference.Name}'");

            await Task.WhenAll(
                ListBlobsResponse(containerReference)
                    .OfType<CloudBlockBlob>()
                    .Select(async blob =>
                    {
                        Console.Write(".");
                        var cloudBlob = await containerReference.GetBlobReferenceFromServerAsync(blob.Name);

                        await cloudBlob.DeleteIfExistsAsync();
                    })
            );
        }

        static IEnumerable<IListBlobItem> ListBlobsResponse(CloudBlobContainer container)
        {
            BlobContinuationToken token = null;

            do
            {
                var segment = container.ListBlobsSegmentedAsync(token).Result;

                token = segment.ContinuationToken;

                foreach (var segmentResult in segment.Results)
                {
                    yield return segmentResult;
                }
            }
            while (token != null);

        }

        static async Task ClearTable(CloudTableClient client, string tableName)
        {
            var table = client.GetTableReference(tableName);

            if (!await table.ExistsAsync()) return;

            Console.WriteLine($"Clearing table '{tableName}'");

            TableContinuationToken token = null;

            while (true)
            {
                var result = await table.ExecuteQuerySegmentedAsync(new TableQuery(), token);

                if (!result.Results.Any())
                {
                    return;
                }

                await Task.WhenAll(
                    result.Results
                        .Select(async row =>
                        {
                            Console.Write(".");
                            await table.ExecuteAsync(TableOperation.Delete(row));
                        })
                );

                token = result.ContinuationToken;
            }
        }


        //public static void DropAndRecreateObjects()
        //{
        //    var cloudTableClient = StorageAccount.CreateCloudTableClient();

        //    var table = cloudTableClient.GetTableReference(TableName);
        //    table.DeleteIfExists();
        //    var cloudBlobClient = StorageAccount.CreateCloudBlobClient();
        //    var container = cloudBlobClient.GetContainerReference(ContainerName.ToLowerInvariant());
        //    container.DeleteIfExists();
        //}

    }
}
