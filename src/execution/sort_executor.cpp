#include "execution/executors/sort_executor.h"
#include <utility>

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
    // Initialize the child executor
    child_executor_->Init();

    // Initialize the result set
    Tuple tuple;
    RID rid;

    // Fetch all the tuples from the child executor
    while (child_executor_->Next(&tuple, &rid)) {
        tuples_.emplace_back(tuple);
    }

    // Get the sorting rule and sort the tuples
    auto order_bys = plan_->GetOrderBy();
    std::sort(tuples_.begin(), tuples_.end(), Comparator(&GetOutputSchema(), order_bys));

    // Initialize the iterator
    iter_ = tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    // If the iterator reaches the end, return false
    if (iter_ == tuples_.end()) {
        return false;
    }

    // Fetch the tuple and increment the iterator
    *tuple = *iter_;
    ++iter_;

    return true;
}

}  // namespace bustub
