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

import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;


//added by zhaowei 20170814
public class IndexSearchCondition {
	private ExprNodeColumnDesc columnDesc;
	private String comparisonOp;
	private ExprNodeConstantDesc constantDesc;
	private ExprNodeGenericFuncDesc comparisonExpr;
	private GenericUDF udf;

	private String[] fields;

	// Support (Not) Between/(Not) In Operator
	private ExprNodeConstantDesc[] multiConstants;
	private boolean isNot;

	public IndexSearchCondition(ExprNodeColumnDesc columnDesc, String comparisonOp,
			ExprNodeConstantDesc[] multiConstants, ExprNodeGenericFuncDesc comparisonExpr, boolean isNot) {
		this(columnDesc, comparisonOp, multiConstants, comparisonExpr, null, isNot);
	}

	public IndexSearchCondition(ExprNodeColumnDesc columnDesc, String comparisonOp,
			ExprNodeConstantDesc[] multiConstants, ExprNodeGenericFuncDesc comparisonExpr, String[] fields,
			boolean isNot) {
		this.columnDesc = columnDesc;
		this.comparisonOp = comparisonOp;
		this.multiConstants = multiConstants;
		this.comparisonExpr = comparisonExpr;
		this.fields = fields;
		this.isNot = isNot;
	}

	public ExprNodeConstantDesc[] getConstantDescs() {
		return multiConstants;
	}

	public ExprNodeConstantDesc getConstantDesc(int index) {
		return multiConstants[index];
	}

	public boolean isNot() {
		return isNot;
	}
	//////////////////////////////////////////////////////////////////////////////

	/**
	 * Constructs a search condition, which takes the form
	 * <p>
	 * 
	 * <pre>
	 * column-ref comparison-op constant-value
	 * </pre>
	 * <p>
	 * .
	 *
	 * @param columnDesc
	 *            column being compared
	 * @param comparisonOp
	 *            comparison operator, e.g. "=" (taken from
	 *            GenericUDFBridge.getUdfName())
	 * @param constantDesc
	 *            constant value to search for
	 * @param comparisonExpr
	 *            the original comparison expression
	 */
	public IndexSearchCondition(ExprNodeColumnDesc columnDesc, String comparisonOp, ExprNodeConstantDesc constantDesc,
			ExprNodeGenericFuncDesc comparisonExpr, String[] fields, GenericUDF udf) {

		this.columnDesc = columnDesc;
		this.comparisonOp = comparisonOp;
		this.constantDesc = constantDesc;
		this.comparisonExpr = comparisonExpr;
		this.fields = fields;
		this.udf = udf;
	}

	public void setColumnDesc(ExprNodeColumnDesc columnDesc) {
		this.columnDesc = columnDesc;
	}

	public ExprNodeColumnDesc getColumnDesc() {
		return columnDesc;
	}

	public void setComparisonOp(String comparisonOp) {
		this.comparisonOp = comparisonOp;
	}

	public String getComparisonOp() {
		return comparisonOp;
	}

	public void setConstantDesc(ExprNodeConstantDesc constantDesc) {
		this.constantDesc = constantDesc;
	}

	public ExprNodeConstantDesc getConstantDesc() {
		return constantDesc;
	}

	public void setComparisonExpr(ExprNodeGenericFuncDesc comparisonExpr) {
		this.comparisonExpr = comparisonExpr;
	}

	public ExprNodeGenericFuncDesc getComparisonExpr() {
		return comparisonExpr;
	}

	public String[] getFields() {
		return fields;
	}

	public GenericUDF getUdf() {
		return udf;
	}

	@Override
	public String toString() {
		return comparisonExpr.getExprString();
	}
}
