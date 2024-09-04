#include "execution/executors/window_function_executor.h"
#include "execution/executors/sort_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Sort() {
    // Check if the window function is empty
    BUSTUB_ASSERT(!plan_->window_functions_.empty(), "Window function couldn't be empty");

    // Initialize the result set
    Tuple tuple;
    RID rid;

    // All clause have the same order by clause, fetch one of them.
    const auto &order_bys = plan_->window_functions_.begin()->second.order_by_;
    const auto &schema = child_executor_->GetOutputSchema();

    // Fetch all the tuples from the child executor
    while (child_executor_->Next(&tuple, &rid)) {
        sorted_.push_back(tuple);
    }

    std::sort(sorted_.begin(), sorted_.end(), Comparator(&schema, order_bys));
}

void WindowFunctionExecutor::PartitionBy(const Tuple &tuple, const WindowFunction &wf,
                                         uint32_t place) {
    // Get the partition key
    const auto &key = GetPartitionKey(&tuple, wf.partition_by_);

    // Get the partition
    auto value = wf.function_->Evaluate(&tuple, child_executor_->GetOutputSchema());

    // Insert the tuple into the partition
    partitions_[place].InsertCombine(key, value);
}

void WindowFunctionExecutor::PartitionAll() {
    // Partition all the tuples
    for (const auto &tuple : sorted_) {
        for (const auto &[place, v] : plan_->window_functions_) {
            PartitionBy(tuple, v, place);
        }
    }
}

void WindowFunctionExecutor::Init() {
    // Initialize the child executor
    child_executor_->Init();
    index_ = 0;

    // Initialize the sorted tuples and partitions
    sorted_.clear();
    partitions_.clear();
    partitions_.resize(plan_->OutputSchema().GetColumnCount());
    partition_all_ = true;

    // Initialize the window functions
    for (uint32_t i = 0; i < partitions_.size(); i++) {
        // Cast the expression to column value expression
        auto expr = dynamic_cast<ColumnValueExpression *>(plan_->columns_[i].get());
        BUSTUB_ASSERT(expr != nullptr, "AbstractExpression cannot convert to column expr");

        // Get the place of the column
        auto place = static_cast<int>(expr->GetColIdx());
        if (place == -1) {
            // If the column is not found, set the type to kINVALID
            auto wf = plan_->window_functions_.at(i);
            partitions_[i].SetType(wf.type_);

            // If the type is not kINVALID, set the function
            if (!wf.order_by_.empty()) {
                partition_all_ = false;
            }
        }
    }

    // Sort the tuples and partition all the tuples
    Sort();
    if (partition_all_) {
        PartitionAll();
    }

    // Initialize the result set
    result_.resize(plan_->output_schema_->GetColumnCount());
}

void WindowFunctionExecutor::Extract(uint32_t result_index, const Tuple *tuple) {
    // Get the window function and the partition key
    auto &wf = plan_->window_functions_.at(result_index);
    auto key = GetPartitionKey(tuple, wf.partition_by_);

    // If the window function is a rank, calculate the rank
    if (wf.type_ == WindowFunctionType::Rank) {
        // Find the partition
        auto iter = partitions_[result_index].Find(key);

        // If the partition is not found, insert the tuple
        Value rank;
        if (iter == partitions_[result_index].End()) {
            uint64_t value = (static_cast<uint64_t>(index_) << 32) + 1;
            partitions_[result_index].InsertCombine(key, ValueFactory::GetBigIntValue(value));
            rank = ValueFactory::GetIntegerValue(1);
        } else {
            // If the partition is found, get the rank
            Value result_index_count = iter->second;
            auto rc = result_index_count.GetAs<uint64_t>();
            const Tuple &pre_one = sorted_[rc >> 32];
            uint32_t count = rc & 0xffffffff;

            // If the tuple is the same as the previous one, the rank is the same
            if (OrderByCmp(wf, pre_one, *tuple)) {
                result_[result_index] = ValueFactory::GetIntegerValue(count);
                rank = ValueFactory::GetIntegerValue(count);
            } else {
                // If the tuple is different from the previous one, increment the rank
                count++;

                // Set the rank and insert the tuple
                uint64_t temp = (static_cast<uint64_t>(index_) << 32) + index_ + 1;
                auto value = ValueFactory::GetBigIntValue(temp);
                partitions_[result_index].InsertCombine(key, value);

                // Update the rank
                rank = ValueFactory::GetIntegerValue(index_ + 1);
            }
        }

        // Set the rank
        result_[result_index] = rank;
        return;
    }

    // If the window function is a row number, calculate the row number
    if (!partition_all_) {
        Value value = wf.function_->Evaluate(tuple, child_executor_->GetOutputSchema());
        partitions_[result_index].InsertCombine(key, value);
    }

    // Get the result
    result_[result_index] = partitions_[result_index].Get(key);
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    // If the index reaches the end, return false   
    if (index_ == sorted_.size()) {
        return false;
    }

    // Extract the tuple
    for (uint32_t i = 0; i < result_.size(); i++) {
        // Cast the expression to column value expression
        auto expr = dynamic_cast<ColumnValueExpression *>(plan_->columns_[i].get());
        BUSTUB_ASSERT(expr != nullptr, "AbstractExpression cannot convert to column expr");

        // Get the place of the column
        auto place = static_cast<int>(expr->GetColIdx());

        // If the column is not found, extract the tuple
        if (place == -1) {  
            Extract(i, &sorted_[index_]);
        } else {
            // If the column is found, evaluate the expression
            result_[i] = plan_->columns_[i]->Evaluate(&sorted_[index_], *plan_->output_schema_);
        }
    }

    // Set the tuple
    *tuple = Tuple{result_, plan_->output_schema_.get()};
    index_++;
    
    return true;
}
}  // namespace bustub
