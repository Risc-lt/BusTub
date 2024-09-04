#include "execution/executors/topn_executor.h"
#include <queue>
#include <vector>

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
    // Initialize the child executor and heap
    child_executor_->Init();
    std::priority_queue<Tuple, std::vector<Tuple>, HeapComparator> heap(HeapComparator(&GetOutputSchema(), plan_->GetOrderBy()));

    // Get the next tuple from the child executor
    Tuple tuple;
    RID rid;

    // Push the tuples to the heap and pop the bigger elements when the heap size is equal to the plan's N
    while (child_executor_->Next(&tuple, &rid)) {
        heap.push(tuple);
        heap_size_++;

        if (heap_size_ > plan_->GetN()) {
            heap.pop();
            heap_size_--;
        }
    }

    // Pop the heap and push it to the stack
    while (!heap.empty()) {
        top_entries_.push(heap.top());
        heap.pop();
    }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    // If the stack is empty, return false
    if (top_entries_.empty()) {
        return false;
    }

    // Pop the top of the stack and set the tuple and rid
    *tuple = top_entries_.top();
    top_entries_.pop();
    return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return top_entries_.size(); };

}  // namespace bustub
