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
#include "common/exception.h"
#include "common/rid.h"
#include "type/value_factory.h"

#define LOG_LEVEL LOG_LEVEL_OFF

#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/logger.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/executors/insert_executor.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"

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
  auto &table_name = exec_ctx_->GetCatalog()->GetTable(table_id)->name_;
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_id);
  index_info_vector_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_name);
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  int32_t inserted_tuple_count = 0;
  auto schema = table_info_->schema_;
  // pull tuple until empty
  while (child_executor_->Next(tuple, rid)) {
    // modified at P4T4.1
    auto txn = exec_ctx_->GetTransaction();
    auto txn_mgr = exec_ctx_->GetTransactionManager();
    auto tmp_ts = txn->GetTransactionTempTs();
    // check index exists or not
    if (index_info_vector_.empty()) {
      auto opt_rid = table_info_->table_->InsertTuple({tmp_ts, false}, *tuple);
      if (!opt_rid.has_value()) {
        return false;
      }
      *rid = opt_rid.value();
      txn->AppendWriteSet(table_info_->oid_, *rid);
      // just set check function to 'nullptr' to skip Write-Write conflict detection
      txn_mgr->UpdateUndoLink(*rid, std::nullopt, nullptr);
      ++inserted_tuple_count;
    } else {
      // update all index for current tuple
      for (auto index_info : index_info_vector_) {
        auto key_schema = index_info->key_schema_;
        auto key_attrs = index_info->index_->GetKeyAttrs();
        auto target_key = tuple->KeyFromTuple(schema, key_schema, key_attrs);
        std::vector<RID> found_rids{};

        if (index_info->is_primary_key_) {
          index_info->index_->ScanKey(target_key, &found_rids, nullptr);
          if (!found_rids.empty()) {
            // maintain primary key index
            txn->SetTainted();
            throw ExecutionException("detect write-write conflict!");
          }
        }

        auto opt_rid = table_info_->table_->InsertTuple({tmp_ts, false}, *tuple);
        if (!opt_rid.has_value()) {
          return false;
        }
        *rid = opt_rid.value();
        txn->AppendWriteSet(table_info_->oid_, *rid);
        // just set check function to 'nullptr' to skip Write-Write conflict detection
        txn_mgr->UpdateUndoLink(*rid, std::nullopt, nullptr);
        ++inserted_tuple_count;

        bool is_index_inserted = index_info->index_->InsertEntry(target_key, *rid, nullptr);
        if (!is_index_inserted) {
          // detect multiple index map to same tuple
          txn->SetTainted();
          throw ExecutionException("detect write-write conflict!");
          return false;
        }
      }
    }
    // insert current tuple
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
