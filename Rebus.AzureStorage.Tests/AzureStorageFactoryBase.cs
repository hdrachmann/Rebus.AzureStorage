﻿using System;
using System.IO;
using Microsoft.WindowsAzure.Storage;
using Rebus.Exceptions;

namespace Rebus.AzureStorage.Tests
{
    public class AzureStorageFactoryBase
    {
        public static string ConnectionString => ConnectionStringFromFileOrNull(Path.Combine(GetBaseDirectory(), "azure_storage_connection_string.txt"))
                                                 ?? ConnectionStringFromEnvironmentVariable("rebus2_storage_connection_string")
                                                 ?? Throw("Could not find Azure Storage connection string!");

        static string GetBaseDirectory()
        {
#if NETSTANDARD1_6
            return AppContext.BaseDirectory;
#else
            return AppDomain.CurrentDomain.BaseDirectory;
#endif
        }
        static string ConnectionStringFromFileOrNull(string filePath)
        {
            if (!File.Exists(filePath))
            {
                Console.WriteLine($"Could not find file {filePath}");
                return null;
            }

            Console.WriteLine($"Using Azure Storage connection string from file {filePath}");
            return File.ReadAllText(filePath);
        }

        static string ConnectionStringFromEnvironmentVariable(string environmentVariableName)
        {
            var value = Environment.GetEnvironmentVariable(environmentVariableName);

            if (value == null)
            {
                Console.WriteLine($"Could not find env variable {environmentVariableName}");
                return null;
            }

            Console.WriteLine($"Using Azure Storage connection string from env variable {environmentVariableName}");

            return value;
        }

        static string Throw(string message)
        {
            throw new RebusApplicationException(message);
        }

        protected static CloudStorageAccount StorageAccount => CloudStorageAccount.Parse(ConnectionString);

        protected static void DropTable(string tableName)
        {
            var client = StorageAccount.CreateCloudTableClient();

            var table = client.GetTableReference(tableName);
            table.DeleteIfExistsAsync().Wait(500);
        }

        protected static void DropContainer(string containerName)
        {
            var client = StorageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            container.DeleteIfExistsAsync().Wait(500);
           
        }
    }
}