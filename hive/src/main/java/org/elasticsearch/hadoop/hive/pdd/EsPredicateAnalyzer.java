package org.elasticsearch.hadoop.hive.pdd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToBinary;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToChar;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToDate;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToDecimal;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToUnixTimeStamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToUtcTimestamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToVarchar;

//added by zhaowei 20170814 
public class EsPredicateAnalyzer {
	private List<String> columnNameList;
	private Set<String> udfNames;
	private Map<String, Set<String>> columnToUDFs;

	public static EsPredicateAnalyzer createAnalzer(List<String> columnNameList) {
		EsPredicateAnalyzer analyzer = new EsPredicateAnalyzer();
		analyzer.udfNames=new HashSet<String>();
		analyzer.columnToUDFs=new HashMap<String, Set<String>>();
		analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual");

		analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic" + ".GenericUDFOPEqualOrGreaterThan");
		analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic" + ".GenericUDFOPEqualOrLessThan");
		analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan");
		analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan");

		analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual");
		// apply !=
		analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween");
		// apply (Not) Between
		analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn"); //
		// apply (Not) In
		analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn"); //
		// apply In
		analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull");
		// apply Null
		analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull");
		// apply Not Null

		analyzer.addComparisonOp("like");

		analyzer.columnNameList = columnNameList;
		if (CollectionUtils.isNotEmpty(analyzer.columnNameList)) {
			for (String name : analyzer.columnNameList) {
				analyzer.allowColumnName(name);
			}
		}
		return analyzer;
	}

	public ExprNodeDesc analyzePredicate(ExprNodeDesc predicate, final List<IndexSearchCondition> searchConditions) {

		Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
		NodeProcessor nodeProcessor = new NodeProcessor() {
			@Override
			public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs)
					throws SemanticException {

				// We can only push down stuff which appears as part of
				// a pure conjunction: reject OR, CASE, etc.
				for (Node ancestor : stack) {
					if (nd == ancestor) {
						break;
					}
					if (!FunctionRegistry.isOpAnd((ExprNodeDesc) ancestor)) {
						return nd;
					}
				}

				return analyzeExpr((ExprNodeGenericFuncDesc) nd, searchConditions, nodeOutputs);
			}
		};

		Dispatcher disp = new DefaultRuleDispatcher(nodeProcessor, opRules, null);
		GraphWalker ogw = new DefaultGraphWalker(disp);
		ArrayList<Node> topNodes = new ArrayList<Node>();
		topNodes.add(predicate);
		HashMap<Node, Object> nodeOutput = new HashMap<Node, Object>();

		try {
			ogw.startWalking(topNodes, nodeOutput);
		} catch (SemanticException ex) {
			throw new RuntimeException(ex);
		}

		ExprNodeDesc residualPredicate = (ExprNodeDesc) nodeOutput.get(predicate);
		return residualPredicate;
	}

	private ExprNodeDesc analyzeExpr(ExprNodeGenericFuncDesc expr, List<IndexSearchCondition> searchConditions,
			Object... nodeOutputs) throws SemanticException {
		GenericUDF genericUDF = expr.getGenericUDF();
		ExprNodeDesc expr1 = (ExprNodeDesc) nodeOutputs[0];
		ExprNodeDesc expr2 = (ExprNodeDesc) nodeOutputs[1];
		if (expr1 == null) {
			return expr1;
		}
		if (expr2 == null) {
			return expr2;
		}
		// We may need to peel off the GenericUDFBridge that is added by CBO or
		// user
		if (expr1.getTypeInfo().equals(expr2.getTypeInfo())) {
			expr1 = getColumnExpr(expr1);
			expr2 = getColumnExpr(expr2);
		}

		ExprNodeDesc[] extracted = ExprNodeDescUtils.extractComparePair(expr1, expr2);

		ExprNodeColumnDesc columnDesc;
		ExprNodeConstantDesc constantDesc;
		if (extracted[0] instanceof ExprNodeConstantDesc) {
			genericUDF = genericUDF.flip();
			columnDesc = (ExprNodeColumnDesc) extracted[1];
			constantDesc = (ExprNodeConstantDesc) extracted[0];
		} else {
			columnDesc = (ExprNodeColumnDesc) extracted[0];
			constantDesc = (ExprNodeConstantDesc) extracted[1];
		}

		Set<String> allowed = columnToUDFs.get(columnDesc.getColumn());
		if (allowed == null) {
			return expr;
		}

		String udfName = genericUDF.getUdfName();
		if (!allowed.contains(genericUDF.getUdfName())) {
			return expr;
		}

		String[] fields = null;
		if (extracted.length > 2) {
			ExprNodeFieldDesc fieldDesc = (ExprNodeFieldDesc) extracted[2];
			// if (!isValidField(fieldDesc)) {
			// return expr;
			// }
			fields = ExprNodeDescUtils.extractFields(fieldDesc);
		}

		// We also need to update the expr so that the index query can be
		// generated.
		// Note that, hive does not support UDFToDouble etc in the query text.
		List<ExprNodeDesc> list = new ArrayList<ExprNodeDesc>();
		list.add(expr1);
		list.add(expr2);
		expr = new ExprNodeGenericFuncDesc(expr.getTypeInfo(), expr.getGenericUDF(), list);

		searchConditions
				.add(new IndexSearchCondition(columnDesc, udfName, constantDesc, expr, fields, expr.getGenericUDF()));
		return fields == null ? null : expr;
	}

	private ExprNodeDesc getColumnExpr(ExprNodeDesc expr) {
		if (expr instanceof ExprNodeColumnDesc) {
			return expr;
		}
		ExprNodeGenericFuncDesc funcDesc = null;
		if (expr instanceof ExprNodeGenericFuncDesc) {
			funcDesc = (ExprNodeGenericFuncDesc) expr;
		}
		if (null == funcDesc) {
			return expr;
		}
		GenericUDF udf = funcDesc.getGenericUDF();
		// check if its a simple cast expression.
		if ((udf instanceof GenericUDFBridge || udf instanceof GenericUDFToBinary || udf instanceof GenericUDFToChar
				|| udf instanceof GenericUDFToVarchar || udf instanceof GenericUDFToDecimal
				|| udf instanceof GenericUDFToDate || udf instanceof GenericUDFToUnixTimeStamp
				|| udf instanceof GenericUDFToUtcTimestamp) && funcDesc.getChildren().size() == 1
				&& funcDesc.getChildren().get(0) instanceof ExprNodeColumnDesc) {
			return expr.getChildren().get(0);
		}
		return expr;
	}

	public void addComparisonOp(String udfName) {
		udfNames.add(udfName);
	}

	public void allowColumnName(String columnName) {
		columnToUDFs.put(columnName, udfNames);
	}
}
