//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_executor.h
//
// Identification: src/include/execution/executors/sort_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * Comparator is a helper class that compares two tuples based on a set of order-bys.
 */
class Comparator {
 public:
  // Constructor
  Comparator() { schema_ = nullptr; }
  Comparator(const Schema *schema, std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys)
      : schema_(schema), order_bys_(std::move(order_bys)) {}

  auto operator()(const Tuple &t1, const Tuple &t2) -> bool {
    for (auto const &order_by : this->order_bys_) {
      const auto order_type = order_by.first;

      // Get the value of the key
      AbstractExpressionRef expr = order_by.second;
      Value v1 = expr->Evaluate(&t1, *schema_);
      Value v2 = expr->Evaluate(&t2, *schema_);

      // Compare the two values based on the order type until find a difference
      if (v1.CompareEquals(v2) == CmpBool::CmpTrue) {
        continue;
      }

      // ASC or DEFAULT
      if (order_type == OrderByType::ASC || order_type == OrderByType::DEFAULT) {
        return v1.CompareLessThan(v2) == CmpBool::CmpTrue;
      }
      // DESC
      return v1.CompareGreaterThan(v2) == CmpBool::CmpTrue;
    }

    // If all the keys are the same, return false
    return false;
  }

 private:
  /** The schema of the tuples to be compared */
  const Schema *schema_;

  /** The order-bys to sort the tuples */
  std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys_;
};

/**
 * The SortExecutor executor executes a sort.
 */
class SortExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SortExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sort plan to be executed
   */
  SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the sort */
  void Init() override;

  /**
   * Yield the next tuple from the sort.
   * @param[out] tuple The next tuple produced by the sort
   * @param[out] rid The next tuple RID produced by the sort
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;

  /** The child executor used to fetch input tuples */
  std::unique_ptr<AbstractExecutor> child_executor_;

  /** The comparator used to sort the tuples */
  std::vector<Tuple> tuples_;
  std::vector<Tuple>::iterator iter_;
};
}  // namespace bustub
