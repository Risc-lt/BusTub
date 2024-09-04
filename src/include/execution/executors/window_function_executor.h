//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.h
//
// Identification: src/include/execution/executors/window_function_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * PartitionKey is a struct that represents a key for partitioning.
 */
struct PartitionKey {
  std::vector<Value> keys_;

  auto operator==(const PartitionKey &rhs) const -> bool {
    for (uint32_t i = 0; i < rhs.keys_.size(); i++) {
      if (keys_[i].CompareEquals(rhs.keys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};
}  // namespace bustub

namespace std {

/** Implements std::hash on PartitionKey */
template <>
struct hash<bustub::PartitionKey> {
  auto operator()(const bustub::PartitionKey &pkey) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : pkey.keys_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};
}  // namespace std

namespace bustub {
/**
 * PartitionHashTable is a hash table that stores the partitioned results of window functions.
 */
class PartitionHashTable {
 public:
  PartitionHashTable() { type_ = static_cast<WindowFunctionType>(0xfffffff); }

  // Insert a key-value pair into the hash table. If the key already exists, update the value.
  auto InsertCombine(const PartitionKey &key, const Value &value) {
    // Check if the key exists
    auto iter = map_->find(key);
    if (iter == map_->end()) {
      Value temp;
      if (type_ == WindowFunctionType::CountAggregate) {
        temp = ValueFactory::GetIntegerValue(0);
      } else {
        temp = ValueFactory::GetNullValueByType(TypeId::INTEGER);
      }
      // Insert the key-value pair
      iter = map_->insert({key, temp}).first;
    }

    // Combine the value with the existing value
    Value temp;
    switch (type_) {
      case WindowFunctionType::CountStarAggregate:
      case WindowFunctionType::CountAggregate:
        if (iter->second.IsNull()) {
          temp = ValueFactory::GetIntegerValue(1);
        } else {
          temp = iter->second.Add(ValueFactory::GetIntegerValue(1));
        }
        break;
      case WindowFunctionType::SumAggregate:
        if (iter->second.IsNull()) {
          temp = value;
        } else {
          temp = iter->second.Add(value);
        }
        break;
      case WindowFunctionType::MinAggregate:
        if (iter->second.IsNull()) {
          temp = value;
        } else {
          temp = iter->second.Min(value);
        }
        break;
      case WindowFunctionType::MaxAggregate:
        if (iter->second.IsNull()) {
          temp = value;
        } else {
          temp = iter->second.Max(value);
        }
        break;
      case WindowFunctionType::Rank:
        temp = value;
        break;
      default:
        throw Exception{"Unknow partition type"};
    }
    iter->second = temp;
    return iter;
  }

  auto SetType(WindowFunctionType type) {
    map_ = std::make_unique<std::unordered_map<PartitionKey, Value>>();
    type_ = type;
  }

  auto Get(const PartitionKey &key) -> Value {
    auto iter = map_->find(key);
    if (iter == map_->end()) {
      throw Exception{"Cannot find key"};
    }
    return iter->second;
  }

  auto Find(const PartitionKey &key) { return map_->find(key); }

  auto End() { return map_->end(); }

 private:
  /** The hash table that stores the partitioned results */
  std::unique_ptr<std::unordered_map<PartitionKey, Value>> map_;
  /** The type of the window function */
  WindowFunctionType type_;
};

/**
 * The WindowFunctionExecutor executor executes a window function for columns using window function.
 *
 * Window function is different from normal aggregation as it outputs one row for each inputing rows,
 * and can be combined with normal selected columns. The columns in WindowFunctionPlanNode contains both
 * normal selected columns and placeholder columns for window functions.
 *
 * For example, if we have a query like:
 *    SELECT 0.1, 0.2, SUM(0.3) OVER (PARTITION BY 0.2 ORDER BY 0.3), SUM(0.4) OVER (PARTITION BY 0.1 ORDER BY 0.2,0.3)
 *      FROM table;
 *
 * The WindowFunctionPlanNode contains following structure:
 *    columns: std::vector<AbstractExpressionRef>{0.1, 0.2, 0.-1(placeholder), 0.-1(placeholder)}
 *    window_functions_: {
 *      3: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.2}
 *        order_by: std::vector<AbstractExpressionRef>{0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.3}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *      4: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.1}
 *        order_by: std::vector<AbstractExpressionRef>{0.2,0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.4}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *    }
 *
 * Your executor should use child executor and exprs in columns to produce selected columns except for window
 * function columns, and use window_agg_indexes, partition_bys, order_bys, functionss and window_agg_types to
 * generate window function columns results. Directly use placeholders for window function columns in columns is
 * not allowed, as it contains invalid column id.
 *
 * Your WindowFunctionExecutor does not need to support specified window frames (eg: 1 preceding and 1 following).
 * You can assume that all window frames are UNBOUNDED FOLLOWING AND CURRENT ROW when there is ORDER BY clause, and
 * UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING when there is no ORDER BY clause.
 *
 */
class WindowFunctionExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new WindowFunctionExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The window aggregation plan to be executed
   */
  WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the window aggregation */
  void Init() override;

  /**
   * Yield the next tuple from the window aggregation.
   * @param[out] tuple The next tuple produced by the window aggregation
   * @param[out] rid The next tuple RID produced by the window aggregation
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the window aggregation plan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The window aggregation plan node to be executed */
  const WindowFunctionPlanNode *plan_;

  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  /** Sort */
  void Sort();
  std::vector<Tuple> sorted_;

  /** Partition by */
  using WindowFunction = WindowFunctionPlanNode::WindowFunction;
  using Partition = std::vector<AbstractExpressionRef>;
  void PartitionBy(const Tuple &tuple, const WindowFunction &wf, uint32_t place);

  void PartitionAll();

  auto GetPartitionKey(const Tuple *tuple, const Partition &partition) {
    std::vector<Value> keys;
    for (const auto &expr : partition) {
      keys.emplace_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    return PartitionKey{std::move(keys)};
  }

  auto CountAsLineNo(const WindowFunctionPlanNode::WindowFunction &wf) {
    return (wf.type_ == WindowFunctionType::CountAggregate || wf.type_ == WindowFunctionType::CountStarAggregate) &&
           !wf.order_by_.empty();
  }

  auto OrderByCmp(const WindowFunctionPlanNode::WindowFunction &wf, const Tuple &lhs, const Tuple &rhs) {
    for (auto [_, expr] : wf.order_by_) {
      Value current = expr->Evaluate(&lhs, child_executor_->GetOutputSchema());
      Value origin = expr->Evaluate(&rhs, child_executor_->GetOutputSchema());
      if (current.CompareEquals(origin) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
  void Extract(uint32_t index, const Tuple *tuple);
  std::vector<PartitionHashTable> partitions_;
  std::vector<Value> result_;
  uint64_t index_;
  bool partition_all_ = true;
};
}  // namespace bustub
