//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
    // Initialize the table info and indexes
    auto catalog = exec_ctx_->GetCatalog();

    table_info_ = catalog->GetTable(plan_->table_oid_);
    indexes_ = catalog->GetTableIndexes(table_info_->name_);
}

void DeleteExecutor::Init() {
    // Initialize the child executor
    child_executor_->Init();
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    // Check if the table info is null
    if (table_info_ == nullptr) {
        return false;
    }

    // Initialize the recording data
    int line_deleted = 0;

    // Volcano model
    while (child_executor_->Next(tuple, rid)) {
        // Mark the old tuple as deleted
        auto old_tuplemeta = table_info_->table_->GetTupleMeta(*rid);
        old_tuplemeta.is_deleted_ = true;
        table_info_->table_->UpdateTupleMeta(old_tuplemeta, *rid);

        // Update the indexes
        for (auto &index : indexes_) {
            // Get the key of the tuple
            auto key_deleted = tuple->KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs());
            // Delete the entry from the index
            index->index_->DeleteEntry(key_deleted, *rid, exec_ctx_->GetTransaction());
        }

        // Increment the number of lines deleted
        line_deleted++;
    }

    // Set the output
    std::vector<Value> values{};
    values.emplace_back(Value(INTEGER, line_deleted));
    *tuple = Tuple(values, &GetOutputSchema());

    return true;
}

}  // namespace bustub
