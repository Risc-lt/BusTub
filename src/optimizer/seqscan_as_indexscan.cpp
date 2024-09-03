#include "execution/plans/abstract_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"
#include "type/value.h"

namespace bustub {

auto Optimizer::FindAnIndexRecursively(const AbstractExpression *expr) -> bool {
  // Check if the current expression is a column value expression
  if (expr->children_.size() == 1) {
    return false;
  }
  // Check if the current expression is a comparison expression
  if (expr->children_.size() != 2) {
    UNREACHABLE("Not exactly contains 2 children.");
  }

  // Get the left and right children of the expression
  auto left = expr->children_[0].get();
  auto right = expr->children_[1].get();
  ValueExpressionType left_type = GetValueExpressionType(left);
  ValueExpressionType right_type = GetValueExpressionType(right);

  // Swap the left and right children if the left child is a column value expression and the right child is a constant
  // value expression
  if (right_type == ValueExpressionType::COLUMN_VALUE && left_type == ValueExpressionType::CONST_VALUE) {
    std::swap(left_type, right_type);
    std::swap(left, right);
  }

  // Check if the left child is a column value expression and the right child is a constant value expression
  if (left_type == ValueExpressionType::COLUMN_VALUE && right_type == ValueExpressionType::CONST_VALUE) {
    if (dynamic_cast<const ComparisonExpression *>(expr) == nullptr) {
      // for the case of EXPLAIN SELECT * FROM t1 WHERE v3 = (0 + v3);
      return false;
    }

    // Increment the number of equality predicates
    eq_count_++;

    // Cast the left child to a column value expression
    auto ptr = dynamic_cast<ColumnValueExpression *>(left);
    if (auto idx_id = FindIndex(ptr); idx_id != -1) {
      pred_key_ = dynamic_cast<ConstantValueExpression *>(right);
      found_index_id_ = idx_id;
      return true;
    }
    return false;
  }

  // Recursively check the left and right children of the expression
  bool result = false;
  if (left_type == ValueExpressionType::UNKNOW) {
    result = FindAnIndexRecursively(left);
  }
  if (right_type == ValueExpressionType::UNKNOW) {
    result |= FindAnIndexRecursively(right);
  }
  return result;
}

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // Apply the optimization to all children of the current plan node
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  // Check if the current plan node is a SeqScanPlanNode
  if (optimized_plan->GetType() == PlanType::SeqScan) {
    // Cast the plan node to a SeqScanPlanNode
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);

    // Return plan if SeqScanPlanNode has no predicate
    if (seq_scan_plan.filter_predicate_ == nullptr) {
      return optimized_plan;
    }

    // Initialize the IndexScanPlanNode
    indexes_ = catalog_.GetTableIndexes(seq_scan_plan.table_name_);
    table_info_ = catalog_.GetTable(seq_scan_plan.table_name_);

    index_id_.resize(table_info_->schema_.GetColumnCount(), -1);

    for (auto &index : indexes_) {
      // Check if the index is a single column index
      if (index->key_schema_.GetColumnCount() != 1) {
        continue;
      }
      // Map the index to the column
      index_id_[table_info_->schema_.GetColIdx(index->key_schema_.GetColumn(0).GetName())] = index->index_oid_;
    }

    // Check if the predicate is a single column predicate
    BUSTUB_ASSERT(seq_scan_plan.filter_predicate_->children_.size() != 1, "the predicate must contains 1 child");
    // Check if the predicate has no children predicates
    if (seq_scan_plan.filter_predicate_->children_.empty()) {
      return optimized_plan;
    }
    // Construct the index scan plan node if the predicate matches the rules
    if (FindAnIndexRecursively(seq_scan_plan.filter_predicate_.get()) && eq_count_ == 1) {
      return std::make_shared<IndexScanPlanNode>(optimized_plan->output_schema_, table_info_->oid_, found_index_id_,
                                                 seq_scan_plan.filter_predicate_, pred_key_);
    }
  }
  return optimized_plan;
}

}  // namespace bustub
