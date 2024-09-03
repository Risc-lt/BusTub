//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <cstdint>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "common/rid.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  // Initialize the child executors
  left_executor_->Init();
  right_executor_->Init();

  // Initialize the left tuple and flags
  RID left_rid;
  left_next_ = left_executor_->Next(&left_tuple_, &left_rid);
  left_done_ = false;
}

auto NestedLoopJoinExecutor::LeftJoinTuple(Tuple *left_tuple) -> Tuple {
  // Construct the new tuple to join
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());

  // Add the left tuple values
  for(uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
    values.push_back(left_tuple->GetValue(&left_executor_->GetOutputSchema(), i));
  }

  // Add the right tuple values as NULL
  for(uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
    values.push_back(ValueFactory::GetNullValueByType(plan_->GetRightPlan()->OutputSchema().GetColumn(i).GetType()));
  }

  return {values, &GetOutputSchema()};
}

auto NestedLoopJoinExecutor::InnerJoinTuple(Tuple *left_tuple, Tuple *right_tuple) -> Tuple {
  // Construct the new tuple to join
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());

  // Add the left tuple values
  for(uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
    values.push_back(left_tuple->GetValue(&left_executor_->GetOutputSchema(), i));
  }

  // Add the right tuple values
  for(uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
    values.push_back(right_tuple->GetValue(&right_executor_->GetOutputSchema(), i));
  }

  return {values, &GetOutputSchema()};
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple right_tuple;

  while (true) {
    // If the left tuple reaches the end, return false
    if (!left_next_) {
      return false;
    }

    // If the right tuple reaches the end
    if (!right_executor_->Next(&right_tuple, rid)) {
      // If left join, perform left join
      if(plan_->GetJoinType() == JoinType::LEFT && !left_done_) {
        *tuple = LeftJoinTuple(&left_tuple_);
        *rid = tuple->GetRid();

        left_done_ = true;
        continue;
      }

      // If not left join, reset the right executor and move to the next left tuple
      right_executor_->Init();
      left_next_ = left_executor_->Next(&left_tuple_, rid);
      left_done_ = false;
      continue;
    }

    // If the right tuple is not NULL, check if the join condition is satisfied
    auto eval = plan_->Predicate()->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                                 right_executor_->GetOutputSchema());

    if (!eval.IsNull() && eval.GetAs<bool>()) {
      *tuple = InnerJoinTuple(&left_tuple_, &right_tuple);
      *rid = tuple->GetRid();

      left_done_ = true;
      return true;
    }
  }
}

}  // namespace bustub
