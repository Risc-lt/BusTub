//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"
#include <cstddef>
#include <utility>

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
    // Initialize the child executor
    child_executor_->Init();

    // Initialize the result set
    Tuple tuple;
    RID rid;
    std::size_t count = 0;
    auto limit = plan_->GetLimit();

    // Fetch all the tuples from the child executor
    while (count < limit && child_executor_->Next(&tuple, &rid)) {
        tuples_.emplace_back(tuple);
        count++;
    }

    // Initialize the iterator
    if(!tuples_.empty()) {
        iter_ = tuples_.begin();
    }

}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    // If the iterator reaches the end, return false
    if (tuples_.empty() || iter_ == tuples_.end()) {
        return false;
    }

    // Fetch the tuple and increment the iterator
    *tuple = *iter_;
    ++iter_;

    return true;
}

}  // namespace bustub
