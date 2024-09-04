//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <cstddef>
#include <cstdint>
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  // Initialize the child executors
  left_child_->Init();
  right_child_->Init();

  // Initialize the left tuple
  left_next_ = left_child_->Next(&left_tuple_, &left_rid_);

  Tuple right_tuple;
  RID right_rid;

  // Initialize the hash table
  jht_ = std::make_unique<SimpleHashJoinHashTable>();

  // Build the hash table
  while (right_child_->Next(&right_tuple, &right_rid)) {
    jht_->InsertKey(GetRightJoinKey(&right_tuple), right_tuple);
  }

  // Get the matching right tuples for the first left tuple
  auto left_key = GetLeftJoinKey(&left_tuple_);
  right_tuples_ = jht_->GetValue(left_key);

  // Initialize the iterator if right_tuples_ is not nullptr
  if (right_tuples_ != nullptr) {
    jht_iter_ = right_tuples_->begin();
    // Set the flag
    join_done_ = true;
  } else {
    join_done_ = false;
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // Continue the loop until we find a tuple that satisfies the join condition
  while (true) {
    //  If the right tuples are not nullptr and the iterator is not at the end
    if (right_tuples_ != nullptr && jht_iter_ != right_tuples_->end()) {
      std::vector<Value> values;
      // Get the right tuple
      Tuple right_tuple = *jht_iter_;

      // Combine the left and right tuples
      for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(left_tuple_.GetValue(&left_child_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(right_tuple.GetValue(&right_child_->GetOutputSchema(), i));
      }

      // Create the tuple
      *tuple = Tuple(values, &GetOutputSchema());
      *rid = tuple->GetRid();

      jht_iter_++;
      return true;
    }

    // If the right tuples are nullptr or the iterator is at the end,
    // and it is left join and join_done_ shows that the left tuple has not matched
    if (plan_->GetJoinType() == JoinType::LEFT && !join_done_) {
      std::vector<Value> values;
      // Combine the left and right tuples
      for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(left_tuple_.GetValue(&left_child_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(ValueFactory::GetNullValueByType(right_child_->GetOutputSchema().GetColumn(i).GetType()));
      }

      // Create the tuple
      *tuple = Tuple(values, &GetOutputSchema());
      *rid = tuple->GetRid();

      // Set the flag
      join_done_ = true;
      return true;
    }

    // If it is not left join or the left tuple has matched
    // Get the next left tuple
    left_next_ = left_child_->Next(&left_tuple_, &left_rid_);
    if (!left_next_) {
      return false;
    }

    // Get the matching right tuples for the left tuple
    auto left_key = GetLeftJoinKey(&left_tuple_);
    right_tuples_ = jht_->GetValue(left_key);

    // Initialize the iterator if right_tuples_ is not nullptr
    if (right_tuples_ != nullptr) {
      jht_iter_ = right_tuples_->begin();
      // Set the flag
      join_done_ = true;
    } else {
      join_done_ = false;
    }
  }
}

}  // namespace bustub
