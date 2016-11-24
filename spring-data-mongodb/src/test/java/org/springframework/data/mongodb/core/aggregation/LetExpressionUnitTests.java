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

import static org.hamcrest.core.Is.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.DBObjectTestUtils;
import org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Let;
import org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Let.ExpressionVariable;
import org.springframework.data.mongodb.core.aggregation.FilterExpressionUnitTests.Sales;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class LetExpressionUnitTests {

	public static final DBObject EXPECTED = (DBObject) JSON.parse("{ \"$let\": {" + //
			"\"vars\": {" + //
			"\"total\": { \"$add\": [ \"$price\", \"$tax\" ] }," + //
			"\"discounted\": { \"$cond\": { \"if\": \"$applyDiscount\", \"then\": 0.9, \"else\": 1.0 } }" + //
			"}," + //
			"\"in\": { \"$multiply\": [ \"$$total\", \"$$discounted\" ] }" + //

			"}}");

	@Mock MongoDbFactory mongoDbFactory;

	private AggregationOperationContext aggregationContext;
	private MongoMappingContext mappingContext;

	@Before
	public void setUp() {

		mappingContext = new MongoMappingContext();
		aggregationContext = new TypeBasedAggregationOperationContext(Sales.class, mappingContext,
				new QueryMapper(new MappingMongoConverter(new DefaultDbRefResolver(mongoDbFactory), mappingContext)));
	}

	/**
	 * @see DATAMONGO-1538
	 */
	@Test
	public void shouldConstructLetExpressionCorrectly() {

		TypedAggregation<Sales> agg = Aggregation.newAggregation(Sales.class,
				Aggregation.project()
						.and(Let.let(AggregationFunctionExpressions.ADD.of(Fields.field("price"), Fields.field("tax"))).as("total") //
								.andExpression(
										ConditionalOperator.newBuilder().when(Fields.field("applyDiscount")).then(0.9D).otherwise(1.0D))
								.as("discounted") //
								.in(AggregationFunctionExpressions.MULTIPLY.of(Fields.field("total"), Fields.field("discounted")))) //
						.as("finalTotal"));

		DBObject dbo = agg.toDbObject("sales", aggregationContext);

		List<Object> pipeline = DBObjectTestUtils.getAsList(dbo, "pipeline");
		DBObject $project = DBObjectTestUtils.getAsDBObject((DBObject) pipeline.get(0), "$project");
		DBObject finalTotal = DBObjectTestUtils.getAsDBObject($project, "finalTotal");

		assertThat(finalTotal, is(EXPECTED));
	}

	/**
	 * @see DATAMONGO-1538
	 */
	@Test
	public void shouldConstructLetExpressionCorrectlyWhenUsingLetOnProjectionBuilder() {

		ExpressionVariable var1 = ExpressionVariable.newVariable("total")
				.forExpression(AggregationFunctionExpressions.ADD.of(Fields.field("price"), Fields.field("tax")));

		ExpressionVariable var2 = ExpressionVariable.newVariable("discounted")
				.forExpression(ConditionalOperator.newBuilder().when(Fields.field("applyDiscount")).then(0.9D).otherwise(1.0D));

		AggregationExpression in = AggregationFunctionExpressions.MULTIPLY.of(Fields.field("total"),
				Fields.field("discounted"));

		TypedAggregation<Sales> agg = Aggregation.newAggregation(Sales.class,
				Aggregation.project().and("foo").let(Arrays.asList(var1, var2), in).as("finalTotal"));

		DBObject dbo = agg.toDbObject("sales", aggregationContext);

		List<Object> pipeline = DBObjectTestUtils.getAsList(dbo, "pipeline");
		DBObject $project = DBObjectTestUtils.getAsDBObject((DBObject) pipeline.get(0), "$project");
		DBObject finalTotal = DBObjectTestUtils.getAsDBObject($project, "finalTotal");

		assertThat(finalTotal, is(EXPECTED));
	}

	static class Sales {

		String id;
		Integer price;
		Float tax;
		boolean applyDiscount;
	}

}
