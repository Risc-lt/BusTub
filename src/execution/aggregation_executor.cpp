//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  // Initialize the child executor and the hash table
  child_executor_->Init();
  aht_.Clear();

  // Iterate over all tuples from the child executor
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    // Create the aggregate key and value
    auto key = MakeAggregateKey(&tuple);
    auto value = MakeAggregateValue(&tuple);

    // Insert the key and value into the hash table
    aht_.InsertCombine(key, value);
  }

  // Check if the hash table is empty
  if (aht_.Size() == 0 && GetOutputSchema().GetColumnCount() == 1) {
    aht_.EmptyInsertCombine();
  }

  // Initialize the hash table iterator
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // Check if the hash table iterator has reached the end
  if (aht_iterator_ == aht_.End()) {
    return false;
  }

  // Get the aggregate key and value from the hash table iterator
  const auto &key = aht_iterator_.Key();
  const auto &value = aht_iterator_.Val();

  // Create the output tuple
  std::vector<Value> values;
  for (const auto &group_by_val : key.group_bys_) {
    values.emplace_back(group_by_val);
  }
  for (const auto &aggregate_val : value.aggregates_) {
    values.emplace_back(aggregate_val);
  }
  *tuple = Tuple(values, &GetOutputSchema());
  *rid = tuple->GetRid();

  // Move the hash table iterator to the next entry
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
