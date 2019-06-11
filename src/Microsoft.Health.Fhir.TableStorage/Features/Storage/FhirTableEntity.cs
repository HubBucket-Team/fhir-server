// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Microsoft.Health.Fhir.TableStorage.Features.Storage
{
    public class FhirTableEntity : TableEntity
    {
        public FhirTableEntity()
        {
            Properties = new Dictionary<string, EntityProperty>();
        }

        public FhirTableEntity(
            string resourceId,
            string versionId,
            string resourceTypeName,
            byte[] rawResourceData,
            string resourceRequestMethod,
            string resourceRequestUri,
            DateTimeOffset lastModified,
            bool isDeleted,
            bool isHistory,
            IDictionary<string, EntityProperty> searchIndicies)
        {
            // PartitionKey = ResourceTypeName
            // RowKey = {ResourceId_Version}

            ResourceId = resourceId;
            VersionId = versionId;
            PartitionKey = resourceTypeName;
            RawResourceData = rawResourceData;
            ResourceRequestMethod = resourceRequestMethod;
            ResourceRequestUri = resourceRequestUri;
            LastModified = lastModified;
            IsDeleted = isDeleted;
            IsHistory = isHistory;

            RowKey = IsHistory ? CreateId(resourceId, versionId) : CreateId(resourceId);
            Properties = searchIndicies;
        }

        public IDictionary<string, EntityProperty> Properties { get; }

        public string ResourceId { get; set; }

        public string VersionId { get; set; }

        public string ResourceTypeName => PartitionKey;

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1819: Properties should not return arrays", Justification = "DTO Entity.")]
        public byte[] RawResourceData { get; set; }

        public string ResourceRequestMethod { get; set; }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1056:Uri parameters should not be strings", Justification = "DTO Entity.")]
        public string ResourceRequestUri { get; set; }

        public DateTimeOffset LastModified { get; set; }

        public bool IsDeleted { get; set; }

        public bool IsHistory { get; set; }

        public override void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            base.ReadEntity(properties, operationContext);

            foreach (var item in properties.Where(x => x.Key.StartsWith("s_", StringComparison.Ordinal)))
            {
                Properties.Add(item);
            }
        }

        public override IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            return base.WriteEntity(operationContext)
                .Concat(Properties)
                .ToDictionary(x => x.Key, x => x.Value);
        }

        public static string CreateId(string resourceId, string versionId = null)
        {
            if (!string.IsNullOrEmpty(versionId))
            {
                return $"{resourceId}_{versionId}";
            }

            return resourceId;
        }
    }
}
