#include "execution/execution_common.h"
#include <optional>
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "execution/expressions/column_value_expression.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto ReconstructValuesFromTuple(const Schema *schema,  
                                const Tuple &tuple) -> std::vector<Value> {
  // Initialize the values vector with the values from the base tuple
  std::vector<Value> values(schema->GetColumnCount());

  // Iterate through the columns in the schema and evaluate the column value expression
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    const auto &col = schema->GetColumn(i);
    ColumnValueExpression expr(0, i, col.GetType());
    values[i] = expr.Evaluate(&tuple, *schema);
  }

  return values;
}

void ApplyModifications(std::vector<Value> &values,                  
                     const Schema *schema, const UndoLog &log) {
  // Initialize the vectors to store the modified columns and their indices
  std::vector<Column> columns;
  std::vector<uint32_t> col_indices;

  // Iterate through the modified fields in the undo log and apply the modifications to the values
  for (uint32_t i = 0; i < log.modified_fields_.size(); i++) {
    // If the field is not modified, skip it
    if (!log.modified_fields_[i]) {
      continue;
    }

    // Otherwise, evaluate the column value expression and update the value
    columns.push_back(schema->GetColumn(i));
    col_indices.push_back(i);
  }

  // Create a new schema with only the modified columns
  Schema new_schema{columns};
  for (uint32_t i = 0; i < new_schema.GetColumnCount(); i++) {
    const auto &col = schema->GetColumn(i);
    ColumnValueExpression expr(0, i, col.GetType());
    values[col_indices[i]] = expr.Evaluate(&log.tuple_, new_schema);
  }
}

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  // if the base tuple is deleted or undo_logs are empty, return nullopt
  if ( base_meta.is_deleted_ && undo_logs.empty()) {
    return std::nullopt;
  }

  // Initialize the values vector with the values from the base tuple
  std::vector<Value> values;
  auto deleted = base_meta.is_deleted_;

  // if the base tuple is deleted, set all values to NULL
  if (deleted) {
    values.resize(schema->GetColumnCount(), Value{});
  } else {
    // Otherwise, reconstruct the values from the base tuple
    values = ReconstructValuesFromTuple(schema, base_tuple);
  }

  // Iterate through the undo logs and apply the modifications to the values
  for (const auto &log : undo_logs) {
    deleted = log.is_deleted_;

    // If the tuple is deleted, set all values to NULL
    if (deleted) {
      for (size_t i = 0; i < schema->GetColumnCount(); i++) {
        values[i] = Value{};
      }
    } else {
      ApplyModifications(values, schema, log);
    }
  }

  // If the final tuple is deleted, return nullopt
  if (deleted) {
    return std::nullopt;
  } 
  
  return Tuple{std::move(values), schema};
}

auto ReconstructFor(TransactionManager *txn_mgr, Transaction *txn, Tuple *tuple,
                    RID rid, TupleMeta &meta, const Schema *schema) -> bool {
  // Get the read timestamp and transaction ID
  auto read_ts = txn->GetReadTs();
  auto txn_id = txn->GetTransactionId();
  auto result = ReconstructValuesFromTuple(schema, *tuple);

  // If the tuple is not temporary, check if the timestamp is less than or equal to the read timestamp
  if (!IsTempTs(meta.ts_)) {
    if (meta.ts_ <= read_ts) {
      return meta.is_deleted_;
    }
  } else {
    if (meta.ts_ == txn->GetTransactionTempTs()) {
      return meta.is_deleted_;
    }
  }

  // Iterate through the version chain and check if the tuple is deleted
  for (VersionChainIter iter{txn_mgr, rid}; !iter.IsEnd(); iter.Next()) {
    auto log = iter.Get();
    BUSTUB_ASSERT(!IsTempTs(log.ts_), "log.ts couldn't be a temp txn timestamp");
    auto curr_txn_id = iter.GetLink().prev_txn_;

    if (log.is_deleted_) {
      for (auto &v : result) {
        v = ValueFactory::GetNullValueByType(v.GetTypeId());
      }
    } else {
      ApplyModifications(result, schema, log);
    }

    if (log.ts_ <= read_ts || curr_txn_id == txn_id) {
      if (!log.is_deleted_) {
        *tuple = Tuple{result, schema};
      }
      return log.is_deleted_;
    }
  }
  return true;
}

void PutHeader(std::stringstream &ss, RID rid, const TupleMeta &meta,  //
               const Tuple &tuple, const Schema *schema) {
  auto rid_info = fmt::format("RID={}/{}", rid.GetPageId(), rid.GetSlotNum());
  std::string txn_info;
  std::string tuple_info;
  if ((meta.ts_ & TXN_START_ID) != 0) {
    txn_info = fmt::format("ts=txn{}", meta.ts_ & ~(TXN_START_ID));
  } else {
    txn_info = fmt::format("ts={}", meta.ts_);
  }

  if (meta.is_deleted_) {
    tuple_info = "<del>";
  } else {
    tuple_info = tuple.ToString(schema);
  }
  ss << rid_info << ' ' << txn_info << ' ' << tuple_info << '\n';
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr,  //
               const TableInfo *table_info, TableHeap *table_heap) {
  // always use stderr for printing logs...
  std::stringstream ss;
  fmt::println(stderr, "debug_hook: {}\n", info);

  for (auto iter = table_heap->MakeIterator(); !iter.IsEnd(); ++iter) {
    auto rid = iter.GetRID();
    auto [meta, tuple] = iter.GetTuple();
    std::vector<Value> values = ReconstructValuesFromTuple(&table_info->schema_, tuple);
    // header
    PutHeader(ss, rid, meta, tuple, &table_info->schema_);
    // chain
    auto link_opt = txn_mgr->GetUndoLink(rid);
    if (!link_opt.has_value()) {
      throw Exception{fmt::format("Cannot get link from the rid{}/{}",  //
                                  rid.GetPageId(),                      //
                                  rid.GetSlotNum())};
    }
    for (VersionChainIter iter{txn_mgr, rid}; !iter.IsEnd(); iter.Next()) {
      auto link = iter.GetLink();
      if (txn_mgr->txn_map_.find(link.prev_txn_) == txn_mgr->txn_map_.end()) {
        ss << "remained will not show since it GCed\n";
        break;
      }
      auto log = iter.Get();
      if (log.is_deleted_) {
        for (auto &v : values) {
          v = Value{};
        }
        ss << fmt::format("  txn{}@{} <del> ts={}\n", GetTxnId(link.prev_txn_),  //
                          link.prev_log_idx_, log.ts_);
      } else {
        ApplyModifications(values, &table_info->schema_, log);
        auto size = log.modified_fields_.size();
        ss << fmt::format("  txn{}@{} ", GetTxnId(link.prev_txn_), link.prev_log_idx_);
        ss << '(';
        for (uint32_t i = 0; i < size; i++) {
          if (log.modified_fields_[i]) {
            ss << values[i].ToString();
          }
          if (i + 1 == size) {
            ss << ')';
          } else {
            ss << ',';
          }
        }
        ss << fmt::format(" ts={}\n", log.ts_);
      }
    }
  }

  fmt::println(stderr, "{}", ss.str());

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

}  // namespace bustub
