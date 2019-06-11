// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Health.Fhir.Core.Features.Persistence;
using Microsoft.Health.Fhir.Core.Features.Search;
using Microsoft.Health.Fhir.Core.Models;
using Microsoft.Health.Fhir.TableStorage.Features.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.TableStorage.Features.Search
{
    public class TableStorageSearchService : SearchService
    {
        private CloudTable _table;

        public TableStorageSearchService(
            ISearchOptionsFactory searchOptionsFactory,
            IFhirDataStore fhirDataStore,
            IModelInfoProvider modelInfoProvider,
            CloudTableClient client)
            : base(searchOptionsFactory, fhirDataStore, modelInfoProvider)
        {
            _table = client.GetTableReference("fhir");
            _table.CreateIfNotExistsAsync().GetAwaiter().GetResult();
        }

        protected override async Task<SearchResult> SearchInternalAsync(
            SearchOptions searchOptions,
            CancellationToken cancellationToken)
        {
            try
            {
                var filters = new List<string>();
                filters.Add(TableQuery.GenerateFilterConditionForBool(nameof(KnownResourceWrapperProperties.IsHistory), "eq", false));
                filters.Add(TableQuery.GenerateFilterConditionForBool(nameof(KnownResourceWrapperProperties.IsDeleted), "eq", false));

                var query = new TableQuery<FhirTableEntity>();

                var expressionQueryBuilder = new ExpressionQueryBuilder(filters);

                if (searchOptions.Expression != null)
                {
                    searchOptions.Expression.AcceptVisitor(expressionQueryBuilder, default);
                }

                query.Where(string.Join(" and ", filters.Select(x => $"({x})")));

                query = query.Take(searchOptions.MaxItemCount);

                if (searchOptions.CountOnly)
                {
                    var count = await RowCount(query);

                    return new SearchResult(count, searchOptions.UnsupportedSearchParams);
                }
                else
                {
                    TableContinuationToken ct = GetContinuationToken(searchOptions);

                    TableQuerySegment<FhirTableEntity> results =
                        await _table.ExecuteQuerySegmentedAsync(query, ct);

                    return new SearchResult(
                        results.Results.Select(TableStorageFhirDataStore.ToResourceWrapper),
                        searchOptions.UnsupportedSearchParams,
                        SerializeContinuationToken(results));
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                throw;
            }
        }

        protected override async Task<SearchResult> SearchHistoryInternalAsync(
            SearchOptions searchOptions,
            CancellationToken cancellationToken)
        {
            var filters = new List<string>();
            var query = new TableQuery<FhirTableEntity>();
            var expressionQueryBuilder = new ExpressionQueryBuilder(filters);

            if (searchOptions.Expression != null)
            {
                searchOptions.Expression.AcceptVisitor(expressionQueryBuilder, default);
            }

            query.Where(string.Join(" and ", filters.Select(x => $"({x})")));

            query = query.Take(searchOptions.MaxItemCount);

            TableContinuationToken ct = GetContinuationToken(searchOptions);

            TableQuerySegment<FhirTableEntity> results = await _table.ExecuteQuerySegmentedAsync(query, ct);

            return new SearchResult(
                results.Results.Select(TableStorageFhirDataStore.ToResourceWrapper),
                searchOptions.UnsupportedSearchParams,
                SerializeContinuationToken(results));
        }

        private static TableContinuationToken GetContinuationToken(SearchOptions searchOptions)
        {
            return string.IsNullOrEmpty(searchOptions.ContinuationToken) ?
                new TableContinuationToken() :
                JsonConvert.DeserializeObject<TableContinuationToken>(Encoding.UTF8.GetString(Convert.FromBase64String(searchOptions.ContinuationToken)));
        }

        private static string SerializeContinuationToken(TableQuerySegment<FhirTableEntity> results)
        {
            return results.ContinuationToken == null ? null : Convert.ToBase64String(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(results.ContinuationToken)));
        }

        public async Task<int> RowCount(TableQuery<FhirTableEntity> query)
        {
            query = query.Select(new List<string> { "PartitionKey", "RowKey", "LastModified" });

            int count = 0;
            TableContinuationToken token = null;
            do
            {
                var result = await _table.ExecuteQuerySegmentedAsync(query, token);
                token = result.ContinuationToken;
                count += result.Count();
            }
            while (token != null);

            return count;
        }
    }
}
