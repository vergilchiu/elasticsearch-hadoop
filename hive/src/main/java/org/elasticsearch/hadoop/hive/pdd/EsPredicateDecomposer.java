/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.hadoop.hive.pdd;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.nlpcn.es4sql.Test;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

//added by zhaowei 20170814
public class EsPredicateDecomposer {

	private static final Log LOG = LogFactory.getLog(EsPredicateDecomposer.class);

	private List<String> columnNameList;
	private boolean calledPPD;

	private String query;

	public static EsPredicateDecomposer create(List<String> columnNameList) {
		return new EsPredicateDecomposer(columnNameList);
	}

	private EsPredicateDecomposer(List<String> columnNameList) {
		this.columnNameList = columnNameList;
	}

	public void decomposePredicate(ExprNodeDesc predicate) {
		// 处理 = != > >= < <= like
		List<IndexSearchCondition> searchConditions = new ArrayList<IndexSearchCondition>();
		EsPredicateAnalyzer analyzer = EsPredicateAnalyzer.createAnalzer(columnNameList);
		analyzer.analyzePredicate(predicate, searchConditions);
		if (CollectionUtils.isNotEmpty(searchConditions)) {
			StringBuilder sql = new StringBuilder("select * from test ");
			StringBuilder where = new StringBuilder();
			for (IndexSearchCondition node : searchConditions) {
				if (where.length() != 0) {
					if (node.getUdf() instanceof GenericUDFOPOr) {
						where.append(" or");
					} else {
						where.append(" and");
					}
				}
				//处理fulltext函数
				ExprNodeGenericFuncDesc comparisonExpr = node.getComparisonExpr();
				if(node.getComparisonOp().equals("org.apache.hadoop.hive.ql.udf.generic.GenericUDFFullText")){
					ExprNodeColumnDesc col = (ExprNodeColumnDesc)comparisonExpr.getChildren().get(0);
					ExprNodeConstantDesc constant = (ExprNodeConstantDesc)comparisonExpr.getChildren().get(1);
					where.append(col.getColumn()).append("='").append(constant.getValue()).append("'");
					continue;
				}
				where.append(comparisonExpr.getExprString());
			}
			if (where.length() != 0) {
				sql.append("where ").append(where);
			} else {
				sql.append(where);
			}
			try {
				System.out.println(sql);
				String sqlToEsQuery = Test.sqlToEsQuery(sql.toString());
				JsonParser jsonParser = new JsonParser();
				JsonElement parse = jsonParser.parse(sqlToEsQuery);
				JsonObject asJsonObject = parse.getAsJsonObject();
				asJsonObject.remove("from");
				asJsonObject.remove("size");
				String string = asJsonObject.toString();
				this.query = string;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public String getQuery() {
		return query;
	}

	public boolean isCalledPPD() {
		return calledPPD;
	}

}
