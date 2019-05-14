﻿// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using EnsureThat;
using Microsoft.Health.Fhir.Core.Extensions;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Core.Features.Operations.Export.Models
{
    /// <summary>
    /// Class to hold metadata for an individual export request.
    /// </summary>
    public class ExportJobRecord
    {
        private const string SecretPrefix = "Export-Destination-";

        public ExportJobRecord(Uri exportRequestUri, IReadOnlyCollection<KeyValuePair<string, string>> requestorClaims = null)
        {
            EnsureArg.IsNotNull(exportRequestUri, nameof(exportRequestUri));

            RequestUri = exportRequestUri;
            RequestorClaims = requestorClaims?.OrderBy(claim => claim.Key, StringComparer.Ordinal).ToList();

            // Default values
            SchemaVersion = 1;
            Id = Guid.NewGuid().ToString();
            Status = OperationStatus.Queued;

            // Compute the hash of the job.
            var hashObject = new
            {
                RequestUri,
                RequestorClaims,
            };

            Hash = JsonConvert.SerializeObject(hashObject).ComputeHash();

            QueuedTime = DateTimeOffset.UtcNow;
            SecretName = SecretPrefix + Id;
        }

        [JsonConstructor]
        protected ExportJobRecord()
        {
        }

        [JsonProperty(JobRecordProperties.Request)]
        public Uri RequestUri { get; private set; }

        [JsonProperty(JobRecordProperties.RequestorClaims)]
        public IReadOnlyCollection<KeyValuePair<string, string>> RequestorClaims { get; private set; }

        [JsonProperty(JobRecordProperties.SecretName)]
        public string SecretName { get; private set; }

        [JsonProperty(JobRecordProperties.Id)]
        public string Id { get; private set; }

        [JsonProperty(JobRecordProperties.Hash)]
        public string Hash { get; private set; }

        [JsonProperty(JobRecordProperties.QueuedTime)]
        public DateTimeOffset QueuedTime { get; private set; }

        [JsonProperty(JobRecordProperties.SchemaVersion)]
        public int SchemaVersion { get; private set; }

        [JsonProperty(JobRecordProperties.Output)]
        public List<ExportFileInfo> Output { get; private set; } = new List<ExportFileInfo>();

        [JsonProperty(JobRecordProperties.Error)]
        public List<ExportFileInfo> Errors { get; private set; } = new List<ExportFileInfo>();

        [JsonProperty(JobRecordProperties.Status)]
        public OperationStatus Status { get; set; }

        [JsonProperty(JobRecordProperties.StartTime)]
        public DateTimeOffset? StartTime { get; set; }

        [JsonProperty(JobRecordProperties.EndTime)]
        public DateTimeOffset? EndTime { get; set; }

        [JsonProperty(JobRecordProperties.CancelledTime)]
        public DateTimeOffset? CancelledTime { get; set; }

        [JsonProperty(JobRecordProperties.NumberOfConsecutiveFailures)]
        public int NumberOfConsecutiveFailures { get; set; }

        [JsonProperty(JobRecordProperties.TotalNumberOfFailures)]
        public int TotalNumberOfFailures { get; set; }

        [JsonProperty(JobRecordProperties.Progress)]
        public ExportJobProgress Progress { get; set; }
    }
}
