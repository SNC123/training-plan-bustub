//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>
#include "type/value_factory.h"

#define LOG_LEVEL LOG_LEVEL_OFF

#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/logger.h"
#include "execution/executors/insert_executor.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

void InsertExecutor::Init() {
  child_executor_->Init();
  auto &table_id = plan_->table_oid_;
  LOG_DEBUG("table id: %d", table_id);
  auto &table_name = exec_ctx_->GetCatalog()->GetTable(table_id)->name_;
  LOG_DEBUG("table name: %s", table_name.c_str());
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_id);
  index_info_vector_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_name);
  LOG_DEBUG("index_info size: %zu", index_info_vector_.size());
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  int32_t inserted_tuple_count = 0;
  auto schema = table_info_->schema_;
  // pull tuple until empty
  while (child_executor_->Next(tuple, rid)) {

    LOG_DEBUG("tuple: %s", tuple->ToString(&schema).c_str());

    // insert current tuple (modified at P4T3.1, ts 0 -> TXN_START_ID + txn_id )
    auto txn = exec_ctx_->GetTransaction();
    auto txn_mgr = exec_ctx_->GetTransactionManager();
    auto tmp_ts = txn->GetTransactionTempTs();
    auto opt_rid = table_info_->table_->InsertTuple({tmp_ts, false}, *tuple);
    if (!opt_rid.has_value()) {
      return false;
    }
    *rid = opt_rid.value();
    txn->AppendWriteSet(table_info_->oid_, *rid);
    // just set check function to 'nullptr' to skip Write-Write conflict detection
    txn_mgr->UpdateUndoLink(*rid,std::nullopt,nullptr);
    ++inserted_tuple_count;

    LOG_DEBUG("index_info size: %zu", index_info_vector_.size());

    // update all index for current tuple
    for (auto index_info : index_info_vector_) {
      auto key_schema = index_info->key_schema_;
      auto key_attrs = index_info->index_->GetKeyAttrs();
      LOG_DEBUG("update RID: %s", rid->ToString().c_str());
      bool is_index_inserted =
          index_info->index_->InsertEntry(tuple->KeyFromTuple(schema, key_schema, key_attrs), *rid, nullptr);
      if (!is_index_inserted) {
        return false;
      }
    }
  }
  if (!is_inserted_) {
    is_inserted_ = true;
    // return count tuple
    auto integer_value = ValueFactory::GetIntegerValue(inserted_tuple_count);
    auto value_vector = std::vector<Value>{integer_value};
    *tuple = Tuple(value_vector, &GetOutputSchema());
    return true;
  }
  return false;
}

}  // namespace bustub
