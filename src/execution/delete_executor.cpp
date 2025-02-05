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
    LOG_DEBUG("tuple: %s", tuple->ToString(&schema).c_str());
    LOG_DEBUG("rid: %s", tuple->GetRid().ToString().c_str());

    auto txn = exec_ctx_->GetTransaction();
    auto txn_mgr = exec_ctx_->GetTransactionManager();

    // if tuple is not in table heap, it must be write-write conflict (think carefully!!!)
    if (tuple->GetRid().GetPageId() == INVALID_PAGE_ID) {
      txn->SetTainted();
      throw ExecutionException("[DeleteExecutor] Detect write-write conflict !");
    }

    auto meta_ts = table_info_->table_->GetTupleMeta(*rid).ts_;
    // check if tuple is being modified
    if (meta_ts == txn->GetTransactionId()) {
      // maintain undo link
      ModifyHeadUndoLog(txn, txn_mgr, schema, *rid, *tuple, *tuple);
      // delete old tuple(just set is_deleted to true)
      table_info_->table_->UpdateTupleMeta({txn->GetTransactionTempTs(), true}, *rid);

    } else {
      meta_ts = table_info_->table_->GetTupleMeta(*rid).ts_;
      if (IsWriteWriteConflict(txn, meta_ts)) {
        UnlockVersionLink(txn_mgr, *rid);
        txn->SetTainted();
        throw ExecutionException("[DeleteExecutor] write-write conflict!");
      }
      // check version link exists or not
      bool is_locked = LockVersionLink(txn_mgr, *rid);
      if (!is_locked) {
        txn->SetTainted();
        throw ExecutionException("[DeleteExecutor] other threads are using version link !");
      }
      meta_ts = table_info_->table_->GetTupleMeta(*rid).ts_;
      if (IsWriteWriteConflict(txn, meta_ts)) {
        UnlockVersionLink(txn_mgr, *rid);
        txn->SetTainted();
        throw ExecutionException("[DeleteExecutor] write-write conflict!");
      }

      // insert one log with full columns into link
      std::vector<bool> modified_fields;
      auto column_count = table_info_->schema_.GetColumnCount();
      for (uint32_t i = 0; i < column_count; ++i) {
        modified_fields.emplace_back(true);
      }
      meta_ts = table_info_->table_->GetTupleMeta(*rid).ts_;
      Tuple re_tuple = table_info_->table_->GetTuple(*rid).second;
      auto new_undo_log = UndoLog{false, modified_fields, re_tuple, meta_ts, UndoLink{}};
      // if (txn_mgr->GetUndoLink(*rid).has_value()) {
      //   new_undo_log.prev_version_ = txn_mgr->GetUndoLink(*rid).value();
      // }
      std::optional<VersionUndoLink> version_link = txn_mgr->GetVersionLink(*rid);
      if (version_link.has_value()) {
        auto prev_link = version_link->prev_;
        new_undo_log.prev_version_ = prev_link;
      }
      std::optional<UndoLink> new_undo_link = txn->AppendUndoLog(new_undo_log);
      auto new_version_link = VersionUndoLink::FromOptionalUndoLink(new_undo_link);
      new_version_link->in_progress_ = true;
      txn_mgr->UpdateVersionLink(*rid, new_version_link);
      // tmp_ts = txn->GetTransactionTempTs();
      table_info_->table_->UpdateTupleMeta({txn->GetTransactionTempTs(), true}, *rid);
      UnlockVersionLink(txn_mgr, *rid);
    }
    txn->AppendWriteSet(table_info_->oid_, *rid);

    ++deleted_tuple_count;
    LOG_DEBUG("index_info size: %zu", index_info_vector_.size());
    // after P4T4.2(included), we don't need to delete index.

    // update all index for current tuple
    // for (auto index_info : index_info_vector_) {
    //   auto key_schema = index_info->key_schema_;
    //   auto key_attrs = index_info->index_->GetKeyAttrs();
    //   // delete index
    //   index_info->index_->DeleteEntry(tuple->KeyFromTuple(schema, key_schema, key_attrs), *rid, nullptr);
    // }
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
