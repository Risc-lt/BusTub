#include <algorithm>
#include <memory>
#include <vector>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"
#include "type/value.h"

namespace bustub {

void ParseAndExpression(const AbstractExpressionRef &predicate,
                        std::vector<AbstractExpressionRef> *left_key_expressions,
                        std::vector<AbstractExpressionRef> *right_key_expressions) {
  // Convert the predicate to a logical expression
  auto logical_predicate = dynamic_cast<LogicExpression *>(predicate.get());

  // If the predicate is not a logical expression, deduce the key expressions
  if(logical_predicate != nullptr){
    // If the predicate is an AND expression, parse the left and right children
    ParseAndExpression(logical_predicate->GetChildAt(0), left_key_expressions, right_key_expressions);
    // Right one
    ParseAndExpression(logical_predicate->GetChildAt(1), left_key_expressions, right_key_expressions);
  }

  // If the predicate is a comparison expression, 
  auto comparison_predicate = dynamic_cast<ComparisonExpression *>(predicate.get());

  if(comparison_predicate != nullptr){
    // Convert the comparison predicate to a column value expression
    auto col_expr = dynamic_cast<ColumnValueExpression *>(comparison_predicate->GetChildAt(0).get());

    if(col_expr->GetTupleIdx() == 0){
      // If the left child is a column value expression, add it to the left key expressions
      left_key_expressions->push_back(comparison_predicate->GetChildAt(0));
      right_key_expressions->push_back(comparison_predicate->GetChildAt(1));
    } else {
      // If the right child is a column value expression, add it to the right key expressions
      left_key_expressions->push_back(comparison_predicate->GetChildAt(1));
      right_key_expressions->push_back(comparison_predicate->GetChildAt(0));
    }
  }
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // Do optimazaion for all children of the plan
  std::vector<AbstractPlanNodeRef> optimized_children;
  for (const auto &child : plan->GetChildren()) {
    optimized_children.push_back(OptimizeNLJAsHashJoin(child));
  }

  // Clone the plan with optimized children
  auto optimized_plan = plan->CloneWithChildren(optimized_children);

  // If the plan is a nested loop join, exract the join predicate and key expressions
  if(optimized_plan->GetType() == PlanType::NestedLoopJoin){
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
  
    // Get the predicate of the nested loop join
    auto predicate = nlj_plan.predicate_;
  
    std::vector<AbstractExpressionRef> left_key_exprs;
    std::vector<AbstractExpressionRef> right_key_exprs;

    // Parse the predicate and extract the key expressions
    ParseAndExpression(predicate, &left_key_exprs, &right_key_exprs);
    return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                              nlj_plan.GetRightPlan(), left_key_exprs, right_key_exprs,
                                              nlj_plan.GetJoinType());
  }

  return optimized_plan;
}

}  // namespace bustub
