﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Newtonsoft.Json;
using Rebus.Auditing.Sagas;
using Rebus.AzureStorage.Transport;
using Rebus.Logging;
using Rebus.Sagas;

namespace Rebus.AzureStorage.Sagas
{
    /// <summary>
    /// Implementation of <see cref="ISagaSnapshotStorage"/> that uses blobs to store saga data snapshots
    /// </summary>
    public class AzureStorageSagaSnapshotStorage : ISagaSnapshotStorage
    {
        static readonly JsonSerializerSettings DataSettings = new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.All };
        static readonly JsonSerializerSettings MetadataSettings = new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.None };
        static readonly Encoding TextEncoding = Encoding.UTF8;

        readonly CloudBlobContainer _container;
        readonly ILog _log;

        /// <summary>
        /// Creates the storage
        /// </summary>
        public AzureStorageSagaSnapshotStorage(CloudStorageAccount storageAccount, IRebusLoggerFactory loggerFactory, string containerName = "RebusSagaStorage")
        {
            if (storageAccount == null) throw new ArgumentNullException(nameof(storageAccount));
            if (loggerFactory == null) throw new ArgumentNullException(nameof(loggerFactory));

            _log = loggerFactory.GetLogger<AzureStorageSagaSnapshotStorage>();
            _container = storageAccount.CreateCloudBlobClient().GetContainerReference(containerName.ToLowerInvariant());
        }

        /// <summary>
        /// Archives the given saga data under its current ID and revision
        /// </summary>
        public async Task Save(ISagaData sagaData, Dictionary<string, string> sagaAuditMetadata)
        {
            var dataRef = $"{sagaData.Id:N}/{sagaData.Revision:0000000000}/data.json";
            var metaDataRef = $"{sagaData.Id:N}/{sagaData.Revision:0000000000}/metadata.json";
            var dataBlob = _container.GetBlockBlobReference(dataRef);
            var metaDataBlob = _container.GetBlockBlobReference(metaDataRef);
            dataBlob.Properties.ContentType = "application/json";
            metaDataBlob.Properties.ContentType = "application/json";
            await dataBlob.UploadTextAsync(JsonConvert.SerializeObject(sagaData, DataSettings), TextEncoding, DefaultAccessCondition, DefaultRequestOptions, DefaultOperationContext);
            await metaDataBlob.UploadTextAsync(JsonConvert.SerializeObject(sagaAuditMetadata, MetadataSettings), TextEncoding, DefaultAccessCondition, DefaultRequestOptions, DefaultOperationContext);
            await dataBlob.SetPropertiesAsync();
            await metaDataBlob.SetPropertiesAsync();
        }

        static OperationContext DefaultOperationContext => new OperationContext();

        static BlobRequestOptions DefaultRequestOptions => new BlobRequestOptions { RetryPolicy = new ExponentialRetry() };

        static AccessCondition DefaultAccessCondition => AccessCondition.GenerateEmptyCondition();

        /// <summary>
        /// Gets all blobs in the snapshot container
        /// </summary>
        public IEnumerable<IListBlobItem> ListAllBlobs()
        {

            BlobContinuationToken token = null;
            do
            {
                var task = _container.ListBlobsSegmentedAsync(null, true, BlobListingDetails.Metadata, null, token,
                    new BlobRequestOptions(), null);
                AsyncHelpers.RunSync(() => task);
                var segment = task.Result;
                token = segment.ContinuationToken;
                foreach (var segmentResult in segment.Results)
                {
                    yield return segmentResult;
                }
            } while (token != null);

        }

        /// <summary>
        /// Creates the blob container if it doesn't exist
        /// </summary>
        public async Task EnsureContainerExists()
        {
            var exists = await _container.ExistsAsync();
            if (!exists)
            {
                _log.Info("Container {0} did not exist - it will be created now", _container.Name);
                await _container.CreateIfNotExistsAsync();
            }
        }

        static Task<string> GetBlobData(CloudBlockBlob cloudBlockBlob)
        {
            return cloudBlockBlob.DownloadTextAsync(TextEncoding, new AccessCondition(),
                new BlobRequestOptions { RetryPolicy = new ExponentialRetry() }, new OperationContext());
        }

        /// <summary>
        /// Loads the saga data with the given id and revision
        /// </summary>
        public async Task<ISagaData> GetSagaData(Guid sagaDataId, int revision)
        {
            var dataRef = $"{sagaDataId:N}/{revision:0000000000}/data.json";
            var dataBlob = _container.GetBlockBlobReference(dataRef);
            var json = await GetBlobData(dataBlob);
            return (ISagaData)JsonConvert.DeserializeObject(json, DataSettings);
        }

        /// <summary>
        /// Loads the saga metadata for the saga with the given id and revision
        /// </summary>
        public async Task<Dictionary<string, string>> GetSagaMetaData(Guid sagaDataId, int revision)
        {
            var metaDataRef = $"{sagaDataId:N}/{revision:0000000000}/metadata.json";
            var metaDataBlob = _container.GetBlockBlobReference(metaDataRef);
            var json = await GetBlobData(metaDataBlob);
            return JsonConvert.DeserializeObject<Dictionary<string, string>>(json, MetadataSettings);
        }

        /// <summary>
        /// Drops/recreates the snapshot container
        /// </summary>
        public async Task DropAndRecreateContainer()
        {
            await _container.DeleteIfExistsAsync();
            await _container.CreateIfNotExistsAsync();
        }
    }
}