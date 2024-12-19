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
#include <cstdint>
#include "common/config.h"
#include "common/exception.h"
#define LOG_LEVEL LOG_LEVEL_OFF
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

    // modified at P4T3.3
    auto txn = exec_ctx_->GetTransaction();
    auto txn_id = txn->GetTransactionId();
    auto txn_mgr = exec_ctx_->GetTransactionManager();
    auto tmp_ts = txn->GetTransactionTempTs(); 

    // if tuple is not in table heap, it must be write-write conflict (think carefully!!!)
    if(tuple->GetRid().GetPageId() == INVALID_PAGE_ID) {
        txn->SetTainted();
        throw ExecutionException("Detect write-write conflict !");    
    }

    auto meta_ts = table_info_->table_->GetTupleMeta(*rid).ts_;
    // auto read_ts = txn->GetReadTs();
    // check if tuple is being modified
    if(meta_ts >= TXN_START_ID) {
      if(meta_ts == txn_id){
        // delete old tuple(just set is_deleted to true)
        table_info_->table_->UpdateTupleMeta({tmp_ts, true}, *rid);
      }
    }else{       
      // insert one log with full columns into link
      std::vector<bool> modified_fields;
      auto column_count = table_info_->schema_.GetColumnCount();
      for(uint32_t i=0;i<column_count;++i){
        modified_fields.emplace_back(true);
      }
      auto prev_link = txn_mgr->GetUndoLink(*rid);
      auto new_undo_log = UndoLog{false, modified_fields, *tuple, meta_ts, {}};
      if(prev_link.has_value()){
        new_undo_log.prev_version_ = prev_link.value();
      }
      // txn_mgr->
      auto new_undo_link = txn->AppendUndoLog(new_undo_log);
      txn_mgr->UpdateUndoLink(*rid, new_undo_link);
      table_info_->table_->UpdateTupleMeta({tmp_ts, true}, *rid);
    }
    txn->AppendWriteSet(table_info_->oid_, *rid);
    
    ++deleted_tuple_count;
    LOG_DEBUG("index_info size: %zu", index_info_vector_.size());
    // update all index for current tuple
    for (auto index_info : index_info_vector_) {
      auto key_schema = index_info->key_schema_;
      auto key_attrs = index_info->index_->GetKeyAttrs();
      // delete index
      index_info->index_->DeleteEntry(tuple->KeyFromTuple(schema, key_schema, key_attrs), *rid, nullptr);
    }
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
