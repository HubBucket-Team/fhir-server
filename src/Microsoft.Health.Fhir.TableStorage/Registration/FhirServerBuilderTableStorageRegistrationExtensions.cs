// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using EnsureThat;
using Microsoft.Extensions.Configuration;
using Microsoft.Health.Extensions.DependencyInjection;
using Microsoft.Health.Fhir.Core.Registration;
using Microsoft.Health.Fhir.TableStorage.Configs;
using Microsoft.Health.Fhir.TableStorage.Features.Search;
using Microsoft.Health.Fhir.TableStorage.Features.Storage;
using Microsoft.WindowsAzure.Storage;

namespace Microsoft.Extensions.DependencyInjection
{
    public static class FhirServerBuilderTableStorageRegistrationExtensions
    {
        public static IFhirServerBuilder AddExperimentalTableStorage(this IFhirServerBuilder fhirServerBuilder, Action<TableStorageDataStoreConfiguration> configureAction = null)
        {
            EnsureArg.IsNotNull(fhirServerBuilder, nameof(fhirServerBuilder));
            IServiceCollection services = fhirServerBuilder.Services;

            services.Add(provider =>
                {
                    var config = new TableStorageDataStoreConfiguration();
                    provider.GetService<IConfiguration>().GetSection("TableStorage").Bind(config);
                    configureAction?.Invoke(config);

                    return config;
                })
                .Singleton()
                .AsSelf();

            services.Add<TableStorageFhirDataStore>()
                .Singleton()
                .AsSelf()
                .AsImplementedInterfaces();

            services.Add<TableStorageSearchService>()
                .Singleton()
                .AsSelf()
                .AsImplementedInterfaces();

            services.Add<TableStoreFhirOperationDataStore>()
                .Singleton()
                .AsSelf()
                .AsImplementedInterfaces();

            services
                .AddHealthChecks()
                .AddCheck<TableHealthServiceCheck>(nameof(TableHealthServiceCheck));

            services.Add(x => CloudStorageAccount.Parse(x.GetRequiredService<TableStorageDataStoreConfiguration>().ConnectionString))
                .Singleton()
                .AsSelf();

            services.Add(x => x.GetRequiredService<CloudStorageAccount>().CreateCloudTableClient())
                .Singleton()
                .AsSelf();

            return fhirServerBuilder;
        }
    }
}
