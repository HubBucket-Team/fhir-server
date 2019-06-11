// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Microsoft.Health.Fhir.Core.Features.Search;
using Microsoft.Health.Fhir.Core.Features.Search.Expressions;
using Microsoft.WindowsAzure.Storage.Table;

namespace Microsoft.Health.Fhir.TableStorage.Features.Search
{
    internal class ExpressionQueryBuilder : IExpressionVisitorWithInitialContext<ExpressionQueryBuilder.Context, object>
    {
        private readonly List<string> _tableQuery;

        public ExpressionQueryBuilder(List<string> tableQuery)
        {
            _tableQuery = tableQuery;
        }

        Context IExpressionVisitorWithInitialContext<Context, object>.InitialContext => default;

        public object VisitSearchParameter(SearchParameterExpression expression, Context context)
        {
            if (expression.Parameter.Name == SearchParameterNames.ResourceType)
            {
                expression.Expression.AcceptVisitor(this, new Context { FieldNameOverride = "PartitionKey" });
            }
            else if (expression.Parameter.Name == SearchParameterNames.LastUpdated)
            {
                expression.Expression.AcceptVisitor(this, new Context { FieldNameOverride = "LastModified" });
            }
            else
            {
                AppendSubquery(expression.Parameter.Name, expression.Expression, context);
            }

            return null;
        }

        public object VisitBinary(BinaryExpression expression, Context context)
        {
            string generateFilterCondition = TableQuery.GenerateFilterCondition(
                $"{context.FieldNameOverride}0",
                GetMappedValue(expression.BinaryOperator),
                expression.Value?.ToString());

            _tableQuery.Add(generateFilterCondition);

            return generateFilterCondition;
        }

        private string GetMappedValue(BinaryOperator expressionBinaryOperator)
        {
            switch (expressionBinaryOperator)
            {
                case BinaryOperator.Equal:
                    return QueryComparisons.Equal;
                case BinaryOperator.GreaterThan:
                    return QueryComparisons.GreaterThan;
                case BinaryOperator.LessThan:
                    return QueryComparisons.LessThan;
                case BinaryOperator.NotEqual:
                    return QueryComparisons.NotEqual;
                case BinaryOperator.GreaterThanOrEqual:
                    return QueryComparisons.GreaterThanOrEqual;
                case BinaryOperator.LessThanOrEqual:
                    return QueryComparisons.LessThanOrEqual;
                default:
                    throw new ArgumentOutOfRangeException(nameof(expressionBinaryOperator));
            }
        }

        public object VisitChained(ChainedExpression expression, Context context)
        {
            throw new System.NotImplementedException();
        }

        public object VisitMissingField(MissingFieldExpression expression, Context context)
        {
            throw new System.NotImplementedException();
        }

        public object VisitMissingSearchParameter(MissingSearchParameterExpression expression, Context context)
        {
            throw new System.NotImplementedException();
        }

        public object VisitMultiary(MultiaryExpression expression, Context context)
        {
            var newContext = new Context
            {
                FieldNameOverride = context.FieldNameOverride,
                MultiOperation = expression.MultiaryOperation.ToString().ToUpperInvariant(),
            };

            foreach (var e in expression.Expressions)
            {
                e.AcceptVisitor(this, newContext);
            }

            if (!string.IsNullOrEmpty(newContext.Filter))
            {
                _tableQuery.Add(newContext.Filter);
            }

            return null;
        }

        public object VisitString(StringExpression expression, Context context)
        {
            string fieldName = GetFieldName(expression, context);

            string value = expression.IgnoreCase ? expression.Value.ToUpperInvariant() : expression.Value;

            if (!expression.IgnoreCase && expression.StringOperator != StringOperator.Equals)
            {
                throw new NotImplementedException();
            }

            switch (expression.StringOperator)
            {
                case StringOperator.Equals:
                    AddFilter(context, TableQuery.GenerateFilterCondition(fieldName, QueryComparisons.Equal, value));
                    break;
                case StringOperator.StartsWith:
                    AddFilter(context, TableQuery.GenerateFilterCondition(fieldName, QueryComparisons.Equal, value));
                    break;
                default:
                    throw new NotImplementedException();
            }

            return null;
        }

        private void AddFilter(Context context, string filter)
        {
            if (!string.IsNullOrEmpty(context.MultiOperation))
            {
                context.CombineFilter(filter);
                _tableQuery.Add(filter);
            }
            else
            {
                _tableQuery.Add(filter);
            }
        }

        public object VisitCompartment(CompartmentSearchExpression expression, Context context)
        {
            throw new System.NotImplementedException();
        }

        private string GetFieldName(IFieldExpression fieldExpression, Context state)
        {
            if (state.FieldNameOverride != null)
            {
                return state.FieldNameOverride;
            }

            return fieldExpression.FieldName.ToString();
        }

        private void AppendSubquery(string parameterName, Expression expression, Context context, bool negate = false)
        {
            if (negate)
            {
                throw new NotImplementedException();
            }

            Trace.WriteLine($"{parameterName}");

            if (expression != null)
            {
                string variable = parameterName.Replace("-", string.Empty, StringComparison.Ordinal);

                var context1 = new Context { FieldNameOverride = context.FieldNameOverride ?? $"s_{variable}0", MultiOperation = context.MultiOperation };

                expression.AcceptVisitor(this, context1);

                if (!string.IsNullOrEmpty(context1.Filter))
                {
                    context.CombineFilter(context1.Filter);
                }
            }
        }

        /// <summary>
        /// Context that is passed through the visit.
        /// </summary>
        internal struct Context
        {
            public string FieldNameOverride { get; set; }

            public string Filter { get; set; }

            public string MultiOperation { get; set; }

            public void CombineFilter(string filter)
            {
                if (string.IsNullOrEmpty(Filter))
                {
                    Filter = $"({filter})";
                }
                else if (!string.IsNullOrEmpty(MultiOperation))
                {
                    Filter = $"{Filter} {MultiOperation} ({filter})";
                }
                else
                {
                    throw new Exception("MultiOperation is null");
                }
            }
        }
    }
}
