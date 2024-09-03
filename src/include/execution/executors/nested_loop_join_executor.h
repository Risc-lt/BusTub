//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.h
//
// Identification: src/include/execution/executors/nested_loop_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * NestedLoopJoinExecutor executes a nested-loop JOIN on two tables.
 */
class NestedLoopJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new NestedLoopJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The nested loop join plan to be executed
   * @param left_executor The child executor that produces tuple for the left side of join
   * @param right_executor The child executor that produces tuple for the right side of join
   */
  NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&left_executor,
                         std::unique_ptr<AbstractExecutor> &&right_executor);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced, not used by nested loop join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the insert */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /**
   * Perform a left join between the left and right tuple.
   * @param left_tuple The left tuple
   * @return The joined tuple
   */
  auto LeftJoinTuple(Tuple *left_tuple) -> Tuple;

  /**
   * Perform an inner join between the left and right tuple.
   * @param left_tuple The left tuple
   * @param right_tuple The right tuple
   * @return The joined tuple
   */
  auto InnerJoinTuple(Tuple *left_tuple, Tuple *right_tuple) -> Tuple;

  /** The NestedLoopJoin plan node to be executed. */
  const NestedLoopJoinPlanNode *plan_;

  /** The child executor that produces tuple for the left side of join. */
  std::unique_ptr<AbstractExecutor> left_executor_;

  /** The child executor that produces tuple for the right side of join. */
  std::unique_ptr<AbstractExecutor> right_executor_;

  /** The current tuple from the left side of join. */
  Tuple left_tuple_;

  /** The flag to indicate if the next tuple from the left side of join is ready. */
  bool left_next_{false};
  bool left_done_{false};
};

}  // namespace bustub
