#include "execution/execution_common.h"
#include <cstdint>
#include <cstdio>
#include <mutex>
#include <optional>
#include <string>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  std::optional<Tuple> result_tuple = base_tuple;
  if (base_meta.is_deleted_) {
    result_tuple = std::nullopt;
  }
  for (auto &log : undo_logs) {
    if (log.is_deleted_) {
      result_tuple = std::nullopt;
      continue;
    }
    // update matched column
    auto base_column_num = schema->GetColumnCount();
    std::vector<Column> columns;
    for (size_t idx = 0; idx < base_column_num; ++idx) {
      if (log.modified_fields_[idx]) {
        columns.emplace_back(schema->GetColumn(idx));
      }
    }
    auto part_schema = Schema(columns);
    auto modified_idx = 0;
    std::vector<Value> values;
    for (size_t idx = 0; idx < base_column_num; ++idx) {
      if (log.modified_fields_[idx]) {
        values.emplace_back(log.tuple_.GetValue(&part_schema, modified_idx++));
      } else {
        // is necessary to check result tuple nullopt???
        values.emplace_back(result_tuple->GetValue(schema, idx));
      }
    }
    result_tuple = Tuple({values, schema});
  }
  return result_tuple;
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {

  // always use stderr for printing logs...
  // store all outputs in a string to avoid being divided into several blocks
  std::string result_string ;
  fmt::println(stderr, "debug_hook: {}", info);

  auto table_iter = table_heap->MakeIterator();
  while( !table_iter.IsEnd() ){
    auto rid = table_iter.GetRID();
    auto tuple = table_iter.GetTuple().second;
    auto tuple_meta = table_iter.GetTuple().first;
    int64_t ts = tuple_meta.ts_;
    auto ts_str = std::to_string(ts);
    if(ts>TXN_START_ID){
      ts_str = fmt::format("txn{}",(ts^TXN_START_ID));
    }
    bool is_del = tuple_meta.is_deleted_;
    std::string del_mark ;
    if(is_del){
      del_mark = "<del marker>";
    }
    // print table heap data
    result_string += fmt::format("RID={}/{} ts={} {} {}\n",
      rid.GetPageId(),rid.GetSlotNum(),  
      ts_str,             
      del_mark,                                       
      tuple.ToString(&table_info->schema_)                                             
    );
    // print log
    auto undo_link = txn_mgr->GetUndoLink(rid);
    while(undo_link!=std::nullopt && undo_link->IsValid()){
      auto undo_log =txn_mgr->GetUndoLog(undo_link.value());
      // build timestamp string
      auto ts_str = undo_log.ts_;
      // build del mark string
      bool is_del = undo_log.is_deleted_;
      std::string del_mark ;
      if(is_del){
        del_mark = "<del>";
      }
      // build previous transaction string
      std::string prev_txn_str = std::to_string(undo_link->prev_txn_^TXN_START_ID);
      // build undo log tuple string
      std::string tuple_str;
      auto base_column_num = table_info->schema_.GetColumnCount();
      std::vector<Column> columns;
      for (size_t idx = 0; idx < base_column_num; ++idx) {
        if (undo_log.modified_fields_[idx]) {
          columns.emplace_back(table_info->schema_.GetColumn(idx));
        }
      }
      auto modified_idx = 0;
      auto part_schema = Schema(columns);
      std::vector<Value> values;
      for (size_t idx = 0; idx < base_column_num; ++idx) {
        if (undo_log.modified_fields_[idx]) {
          tuple_str += undo_log.tuple_.GetValue(&part_schema, modified_idx++).ToString();
        } else {
          tuple_str +="_";
        }
        if(idx != base_column_num-1){
          tuple_str += ", ";
        }
      }
      result_string += fmt::format("  txn{}@ ts={} {} ({}) \n",
        prev_txn_str,
        ts_str,  
        del_mark,                                                 
        tuple_str                                                    
      );
      undo_link = undo_log.prev_version_;
    }
    ++table_iter;
  }
  fmt::println(stderr,"{}",result_string);
  // fmt::println(
  //     stderr,
  //     "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
  //     "finished task 2. Implementing this helper function will save you a lot of time for debugging in later tasks.");

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
