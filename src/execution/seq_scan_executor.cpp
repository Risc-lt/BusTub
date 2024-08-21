//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <cstddef>
#include <memory>
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_(plan), iter_(nullptr) {}

void SeqScanExecutor::Init() {
    // Get the table info from the catalog
    auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
    schema_ = &table_info->schema_;
    // Get the table iterator
    iter_ = std::make_unique<TableIterator>(table_info->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    for(; iter_->IsEnd(); ++(*iter_)){
        // Get the tuple and the meta data
        auto [meta, tup] = iter_->GetTuple();
        
        // Check if the tuple is deleted
        if(meta.is_deleted_){
            continue;
        }

        // Update the output value
        *tuple = tup;
        *rid = iter_->GetRID();

        // Check if the tuple satisfies the predicate
        if(plan_->filter_predicate_ != nullptr){
            // Evaluate the predicate
            auto eval = plan_->filter_predicate_->Evaluate(&tup, *schema_);
            
            if(!eval.GetAs<bool>()){
                continue;
            }
        }

        // Increment the iterator for next iteration
        ++(*iter_);

        return true;
    }

    return false;
}

}  // namespace bustub
