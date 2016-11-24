/*
 * Copyright 2016. the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.aggregation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.FieldReference;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * @author Christoph Strobl
 * @since 1.10
 */
public interface AggregationExpressions {

	/**
	 * {@code $filter} {@link AggregationExpression} allows to select a subset of the array to return based on the
	 * specified condition.
	 *
	 * @author Christoph Strobl
	 * @since 1.10
	 */
	class Filter implements AggregationExpression {

		private Object input;
		private ExposedField as;
		private Object condition;

		private Filter() {
			// used by builder
		}

		/**
		 * Set the {@literal field} to apply the {@code $filter} to.
		 *
		 * @param field must not be {@literal null}.
		 * @return never {@literal null}.
		 */
		public static AsBuilder filter(String field) {

			Assert.notNull(field, "Field must not be null!");
			return filter(Fields.field(field));
		}

		/**
		 * Set the {@literal field} to apply the {@code $filter} to.
		 *
		 * @param field must not be {@literal null}.
		 * @return never {@literal null}.
		 */
		public static AsBuilder filter(Field field) {

			Assert.notNull(field, "Field must not be null!");
			return new FilterExpressionBuilder().filter(field);
		}

		/**
		 * Set the {@literal values} to apply the {@code $filter} to.
		 *
		 * @param values must not be {@literal null}.
		 * @return
		 */
		public static AsBuilder filter(List<?> values) {

			Assert.notNull(values, "Values must not be null!");
			return new FilterExpressionBuilder().filter(values);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpression#toDbObject(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
		 */
		@Override
		public DBObject toDbObject(final AggregationOperationContext context) {

			return toFilter(new ExposedFieldsAggregationOperationContext(ExposedFields.from(as), context) {

				@Override
				public FieldReference getReference(Field field) {

					FieldReference ref = null;
					try {
						ref = context.getReference(field);
					} catch (Exception e) {
						// just ignore that one.
					}
					return ref != null ? ref : super.getReference(field);
				}
			});
		}

		private DBObject toFilter(AggregationOperationContext context) {

			DBObject filterExpression = new BasicDBObject();

			filterExpression.putAll(context.getMappedObject(new BasicDBObject("input", getMappedInput(context))));
			filterExpression.put("as", as.getTarget());

			filterExpression.putAll(context.getMappedObject(new BasicDBObject("cond", getMappedCondition(context))));

			return new BasicDBObject("$filter", filterExpression);
		}

		private Object getMappedInput(AggregationOperationContext context) {
			return input instanceof Field ? context.getReference((Field) input).toString() : input;
		}

		private Object getMappedCondition(AggregationOperationContext context) {

			if (!(condition instanceof AggregationExpression)) {
				return condition;
			}

			NestedDelegatingExpressionAggregationOperationContext nea = new NestedDelegatingExpressionAggregationOperationContext(
					context);
			return ((AggregationExpression) condition).toDbObject(nea);
		}

		/**
		 * @author Christoph Strobl
		 */
		public interface InputBuilder {

			/**
			 * Set the {@literal values} to apply the {@code $filter} to.
			 *
			 * @param array must not be {@literal null}.
			 * @return
			 */
			AsBuilder filter(List<?> array);

			/**
			 * Set the {@literal field} holding an array to apply the {@code $filter} to.
			 *
			 * @param field must not be {@literal null}.
			 * @return
			 */
			AsBuilder filter(Field field);
		}

		/**
		 * @author Christoph Strobl
		 */
		public interface AsBuilder {

			/**
			 * Set the {@literal variableName} for the elements in the input array.
			 *
			 * @param variableName must not be {@literal null}.
			 * @return
			 */
			ConditionBuilder as(String variableName);
		}

		/**
		 * @author Christoph Strobl
		 */
		public interface ConditionBuilder {

			/**
			 * Set the {@link AggregationExpression} that determines whether to include the element in the resulting array.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			Filter by(AggregationExpression expression);

			/**
			 * Set the {@literal expression} that determines whether to include the element in the resulting array.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			Filter by(String expression);

			/**
			 * Set the {@literal expression} that determines whether to include the element in the resulting array.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			Filter by(DBObject expression);
		}

		/**
		 * @author Christoph Strobl
		 */
		static final class FilterExpressionBuilder implements InputBuilder, AsBuilder, ConditionBuilder {

			private final Filter filter;

			FilterExpressionBuilder() {
				this.filter = new Filter();
			}

			public static InputBuilder newBuilder() {
				return new FilterExpressionBuilder();
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Filter.InputBuilder#filter(java.util.List)
			 */
			@Override
			public AsBuilder filter(List<?> array) {

				Assert.notNull(array, "Array must not be null!");
				filter.input = new ArrayList<Object>(array);
				return this;
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Filter.InputBuilder#filter(org.springframework.data.mongodb.core.aggregation.Field)
			 */
			@Override
			public AsBuilder filter(Field field) {

				Assert.notNull(field, "Field must not be null!");
				filter.input = field;
				return this;
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Filter.AsBuilder#as(java.lang.String)
			 */
			@Override
			public ConditionBuilder as(String variableName) {

				Assert.notNull(variableName, "Variable name  must not be null!");
				filter.as = new ExposedField(variableName, true);
				return this;
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Filter.ConditionBuilder#by(org.springframework.data.mongodb.core.aggregation.AggregationExpression)
			 */
			@Override
			public Filter by(AggregationExpression condition) {

				Assert.notNull(condition, "Condition must not be null!");
				filter.condition = condition;
				return filter;
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Filter.ConditionBuilder#by(java.lang.String)
			 */
			@Override
			public Filter by(String expression) {

				Assert.notNull(expression, "Expression must not be null!");
				filter.condition = expression;
				return filter;
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Filter.ConditionBuilder#by(com.mongodb.DBObject)
			 */
			@Override
			public Filter by(DBObject expression) {

				Assert.notNull(expression, "Expression must not be null!");
				filter.condition = expression;
				return filter;
			}
		}
	}

	/**
	 * {@code $let} binds {@link AggregationExpression} to variables for use in the specified {@code in} expression, and
	 * returns the result of the expression.
	 *
	 * @author Christoph Strobl
	 * @since 1.10
	 */
	class Let implements AggregationExpression {

		private List<ExpressionVariable> vars;
		private Object in;

		private Let() {
			vars = new ArrayList<ExpressionVariable>(5);
		}

		/**
		 * Set the {@link AggregationExpression} for one of the variables accessible in the {@code in} expression.
		 *
		 * @param expression must not be {@literal null}.
		 * @return never {@literal null}.
		 */
		public static AsBuilder let(AggregationExpression expression) {
			return new AggregationExpressionBuilder().expression(expression);
		}

		/**
		 * Set the variables accessible in the {@code in} expression.
		 *
		 * @param variables must not be {@literal null}.
		 * @return never {@literal null}.
		 */
		public static InAndExpressionBuilder vars(Collection<ExpressionVariable> variables) {

			AggregationExpressionBuilder builder = new AggregationExpressionBuilder();
			for (ExpressionVariable variable : variables) {
				builder.variableMap.put(variable.variableName, variable);
			}
			return builder;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpression#toDbObject(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
		 */
		@Override
		public DBObject toDbObject(final AggregationOperationContext context) {

			return toLet(new ExposedFieldsAggregationOperationContext(
					ExposedFields.synthetic(Fields.fields(getVariableNames())), context) {

				@Override
				public FieldReference getReference(Field field) {

					FieldReference ref = null;
					try {
						ref = context.getReference(field);
					} catch (Exception e) {
						// just ignore that one.
					}
					return ref != null ? ref : super.getReference(field);
				}
			});
		}

		private String[] getVariableNames() {

			String[] varNames = new String[this.vars.size()];
			for (int i = 0; i < this.vars.size(); i++) {
				varNames[i] = this.vars.get(i).variableName;
			}
			return varNames;
		}

		private DBObject toLet(AggregationOperationContext context) {

			DBObject letExpression = new BasicDBObject();

			DBObject mappedVars = new BasicDBObject();
			for (ExpressionVariable var : this.vars) {
				mappedVars.putAll(getMappedVariable(var, context));
			}

			letExpression.put("vars", mappedVars);
			letExpression.put("in", getMappedIn(context));

			return new BasicDBObject("$let", letExpression);
		}

		private DBObject getMappedVariable(ExpressionVariable var, AggregationOperationContext context) {

			return new BasicDBObject(var.variableName, var.expression instanceof AggregationExpression
					? ((AggregationExpression) var.expression).toDbObject(context) : var.expression);
		}

		private Object getMappedIn(AggregationOperationContext context) {

			if (!(in instanceof AggregationExpression)) {
				return in;
			}

			NestedDelegatingExpressionAggregationOperationContext nea = new NestedDelegatingExpressionAggregationOperationContext(
					context);
			return ((AggregationExpression) in).toDbObject(nea);
		}

		/**
		 * @author Christoph Strobl
		 */
		public static class ExpressionVariable {

			private final String variableName;
			private final Object expression;

			/**
			 * Creates new {@link ExpressionVariable}.
			 *
			 * @param variableName can be {@literal null}.
			 * @param expression can be {@literal null}.
			 */
			private ExpressionVariable(String variableName, Object expression) {

				this.variableName = variableName;
				this.expression = expression;
			}

			/**
			 * Create a new {@link ExpressionVariable} with given name.
			 *
			 * @param variableName must not be {@literal null}.
			 * @return never {@literal null}.
			 */
			public static ExpressionVariable newVariable(String variableName) {

				Assert.notNull(variableName, "VariableName must not be null!");
				return new ExpressionVariable(variableName, null);
			}

			/**
			 * Create a new {@link ExpressionVariable} with current name and given {@literal expression}.
			 *
			 * @param expression must not be {@literal null}.
			 * @return never {@literal null}.
			 */
			public ExpressionVariable forExpression(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				return new ExpressionVariable(variableName, expression);
			}

			/**
			 * Create a new {@link ExpressionVariable} with current name and given {@literal expressionObject}.
			 *
			 * @param expressionObject must not be {@literal null}.
			 * @return never {@literal null}.
			 */
			public ExpressionVariable forExpression(DBObject expressionObject) {

				Assert.notNull(expressionObject, "Expression must not be null!");
				return new ExpressionVariable(variableName, expressionObject);
			}
		}

		/**
		 * @author Christoph Strobl
		 */
		public interface ExpressionBuilder {

			/**
			 * Set the {@link AggregationExpression} for one of the variables accessible in the {@code in} expression.
			 *
			 * @param expression must not be {@literal null}.
			 * @return never {@literal null}.
			 */
			AsBuilder expression(AggregationExpression expression);

			/**
			 * Set the {@link DBObject} representing the expression for one of the variables accessible in the {@code in}
			 * expression.
			 *
			 * @param expression must not be {@literal null}.
			 * @return never {@literal null}.
			 */
			AsBuilder expression(DBObject expression);
		}

		/**
		 * @author Christoph Strobl
		 */
		public interface AsBuilder {

			/**
			 * Set the {@literal variable name} for the {@link AggregationExpression}.
			 *
			 * @param variableName must not be {@literal null}.
			 * @return never {@literal null}.
			 */
			InAndExpressionBuilder as(String variableName);
		}

		/**
		 * @author Christoph Strobl
		 */
		public interface InAndExpressionBuilder {

			/**
			 * Set another {@link AggregationExpression} for one of the variables accessible in the {@code in} expression.
			 *
			 * @param expression must not be {@literal null}.
			 * @return never {@literal null}.
			 */
			AsBuilder andExpression(AggregationExpression expression);

			/**
			 * Set another {@link DBObject} representing the expression for one of the variables accessible in the {@code in}
			 * expression.
			 *
			 * @param expression must not be {@literal null}.
			 * @return never {@literal null}.
			 */
			AsBuilder andExpression(DBObject expression);

			/**
			 * Set the {@link AggregationExpression} to evaluate.
			 *
			 * @param expression must not be {@literal null}.
			 * @return never {@literal null}.
			 */
			Let in(AggregationExpression expression);
		}

		/**
		 * @author Christoph Strobl
		 */
		static class AggregationExpressionBuilder implements AsBuilder, ExpressionBuilder, InAndExpressionBuilder {

			private Map<String, ExpressionVariable> variableMap;

			AggregationExpressionBuilder() {
				variableMap = new LinkedHashMap<String, ExpressionVariable>();
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Let.ExpressionBuilder#expression(org.springframework.data.mongodb.core.aggregation.AggregationExpression)
			 */
			@Override
			public AsBuilder expression(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");
				variableMap.put(null, new ExpressionVariable(null, expression));
				return this;
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Let.ExpressionBuilder#expression(com.mongodb.DBObject)
			 */
			@Override
			public AsBuilder expression(DBObject expression) {

				Assert.notNull(expression, "Expression must not be null!");
				variableMap.put(null, new ExpressionVariable(null, expression));
				return this;
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Let.AsBuilder#as(java.lang.String)
			 */
			@Override
			public InAndExpressionBuilder as(String variableName) {

				Assert.notNull(variableName, "VariableName must not be null!");
				variableMap.put(variableName, new ExpressionVariable(variableName, variableMap.remove(null).expression));
				return this;
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Let.Builder#andExpression(org.springframework.data.mongodb.core.aggregation.AggregationExpression)
			 */
			@Override
			public AsBuilder andExpression(AggregationExpression expression) {
				return expression(expression);
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Let.Builder#andExpression(com.mongodb.DBObject)
			 */
			@Override
			public AsBuilder andExpression(DBObject expression) {
				return expression(expression);
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Let.Builder#in(org.springframework.data.mongodb.core.aggregation.AggregationExpression)
			 */
			@Override
			public Let in(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null!");

				Let instance = new Let();
				instance.vars = new ArrayList(variableMap.values());
				instance.in = expression;
				return instance;
			}
		}
	}
}
