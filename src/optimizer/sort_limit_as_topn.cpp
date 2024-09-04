#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // Do optimization on all children
  std::vector<bustub::AbstractPlanNodeRef> optimized_children;
  for (const auto &child : plan->GetChildren()) {
    optimized_children.emplace_back(OptimizeSortLimitAsTopN(child));
  }

  // Clone the plan with optimized children
  auto optimized_plan = plan->CloneWithChildren(std::move(optimized_children));

  // Check if the plan is a limit plan with a sort child
  if (optimized_plan->GetType() == PlanType::Limit) {
    // Cast the plan to a limit plan
    const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);

    // Check if the child is a sort plan
    auto child = optimized_plan->children_[0];
    if (child->GetType() == PlanType::Sort) {
      // Cast the child to a sort plan
      const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*child);
      return std::make_shared<TopNPlanNode>(optimized_plan->output_schema_, optimized_plan->children_[0],
                                            sort_plan.GetOrderBy(), limit_plan.limit_);
    }
  }
  return optimized_plan;
}

}  // namespace bustub
