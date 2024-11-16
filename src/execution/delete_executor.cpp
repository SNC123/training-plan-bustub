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
#include <memory>

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
    // delete old tuple(just set is_deleted to true)
    table_info_->table_->UpdateTupleMeta({0, true}, *rid);
    ++deleted_tuple_count;
    LOG_DEBUG("index_info size: %zu", index_info_vector_.size());
    // update all index for current tuple
    for (auto index_info : index_info_vector_) {
      auto key_schema = index_info->key_schema_;
      auto key_attrs = index_info->index_->GetKeyAttrs();
      // deleted index
      index_info->index_->DeleteEntry(
        tuple->KeyFromTuple(schema, key_schema, key_attrs), *rid, nullptr
      );
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
