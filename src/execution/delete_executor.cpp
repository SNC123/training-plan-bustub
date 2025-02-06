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
#define LOG_LEVEL LOG_LEVEL_OFF
#include <cstdint>
#include "common/config.h"
#include "common/exception.h"
#include "execution/execution_common.h"

#include <memory>
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

void DeleteExecutor::Init() {
  child_executor_->Init();
  auto table_id = plan_->GetTableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_id);
  auto &table_name = exec_ctx_->GetCatalog()->GetTable(table_id)->name_;
  index_info_vector_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_name);
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  int32_t deleted_tuple_count = 0;
  auto schema = table_info_->schema_;
  // pull tuple until empty
  while (child_executor_->Next(tuple, rid)) {
    auto txn = exec_ctx_->GetTransaction();
    auto txn_mgr = exec_ctx_->GetTransactionManager();

    DeleteTuple(txn, txn_mgr, table_info_, *rid, *tuple);
    ++deleted_tuple_count;
  }
  if (!is_deleted_) {
    is_deleted_ = true;
    // return count tuple
    auto integer_value = ValueFactory::GetIntegerValue(deleted_tuple_count);
    auto value_vector = std::vector<Value>{integer_value};
    *tuple = Tuple(value_vector, &GetOutputSchema());
    return true;
  }
  return false;
}

}  // namespace bustub
