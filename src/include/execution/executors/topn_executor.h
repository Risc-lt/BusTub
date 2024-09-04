//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <memory>
#include <stack>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
/**
 * HeapComparator is a comparator class used to compare two tuples based on a schema and a set of order-bys.
 */
class HeapComparator {
 public:
  // Constructor
  HeapComparator() { schema_ = nullptr; }
  HeapComparator(const Schema *schema, std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys)
      : schema_(schema), order_bys_(std::move(order_bys)) {}

  auto operator()(const Tuple &t1, const Tuple &t2) -> bool {
    for (auto const &order_by : this->order_bys_) {
      // Get the order type
      const auto order_type = order_by.first;

      // Get the value of the expression for the two tuples
      AbstractExpressionRef expr = order_by.second;
      Value v1 = expr->Evaluate(&t1, *schema_);
      Value v2 = expr->Evaluate(&t2, *schema_);

      // Compare the two values based on the order type until find a difference
      if (v1.CompareEquals(v2) == CmpBool::CmpTrue) {
        continue;
      }

      // ASC and DEFAULT
      if (order_type == OrderByType::ASC || order_type == OrderByType::DEFAULT) {
        return v1.CompareLessThan(v2) == CmpBool::CmpTrue;
      }
      // DESC
      return v1.CompareGreaterThan(v2) == CmpBool::CmpTrue;
    }

    // If all the values are equal, return false
    return false;
  }

 private:
  /** The schema of the tuples */
  const Schema *schema_;
  /** The order-bys to compare the tuples */
  std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys_;
};

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The TopN plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the TopN */
  void Init() override;

  /**
   * Yield the next tuple from the TopN.
   * @param[out] tuple The next tuple produced by the TopN
   * @param[out] rid The next tuple RID produced by the TopN
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the TopN */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /** Sets new child executor (for testing only) */
  void SetChildExecutor(std::unique_ptr<AbstractExecutor> &&child_executor) {
    child_executor_ = std::move(child_executor);
  }

  /** @return The size of top_entries_ container, which will be called on each child_executor->Next(). */
  auto GetNumInHeap() -> size_t;

 private:
  /** The TopN plan node to be executed */
  const TopNPlanNode *plan_;

  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  /** The tuples to be limited */
  std::stack<Tuple> top_entries_;

  /** Size of the heap */
  std::size_t heap_size_{0};
};
}  // namespace bustub
