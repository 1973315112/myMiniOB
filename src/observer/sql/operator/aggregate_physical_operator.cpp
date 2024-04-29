#include <math.h>
#include "common/log/log.h"
#include "sql/operator/aggregate_physical_operator.h"
#include "storage/record/record.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

RC AggregatePhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  std::unique_ptr<PhysicalOperator> &child = children_[0];
  RC rc = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  //trx_ = trx;

  return RC::SUCCESS;
}

RC AggregatePhysicalOperator::next()
{
  if(result_tuple_.cell_num()>0) return RC::RECORD_EOF;  //已聚合
  
  RC rc = RC::SUCCESS;
  PhysicalOperator *oper = children_[0].get();
  
  int count=0,opnumber=(int)aggregations_.size();
  std::vector<Value> result_cells(opnumber);
  
  while (RC::SUCCESS == (rc = oper->next())) //对应count个元组
  {
    count++;
    Tuple *tuple = oper->current_tuple();
    for(int cell_idx=0;cell_idx<opnumber;cell_idx++)//对应opnumber个操作
    {
      const AggrOp aggregation=aggregations_[cell_idx];
      Value cell;
      rc=tuple->cell_at(cell_idx,cell);
      AttrType attr_type=cell.attr_type();
      if(count==1) 
      {
        result_cells[cell_idx].set_value(cell);
        continue;
      }
      
      switch (aggregation)
      {
        case AggrOp::AGGR_SUM:
        case AggrOp::AGGR_AVG:
          if(attr_type==AttrType::INTS || attr_type==AttrType::FLOATS) result_cells[cell_idx].set_float(result_cells[cell_idx].get_float()+cell.get_float());
        break;
        case AggrOp::AGGR_MIN:
        	if(result_cells[cell_idx].compare(cell)>0) result_cells[cell_idx].set_value(cell);
        break;
        case AggrOp::AGGR_MAX:
          if(result_cells[cell_idx].compare(cell)<0) result_cells[cell_idx].set_value(cell);           
        break;
        case AggrOp::AGGR_COUNT:
        case AggrOp::AGGR_COUNT_ALL:
          break;                                                                          
        default:
          return RC::UNIMPLENMENT;
      }
    }
  }
  for(int i=0;i<opnumber;i++)
  {
    const AggrOp aggregation=aggregations_[i];
  	if(aggregation==AggrOp::AGGR_AVG) result_cells[i].set_float(result_cells[i].get_float()/count);
    else if(aggregation==AggrOp::AGGR_COUNT||aggregation==AggrOp::AGGR_COUNT_ALL) result_cells[i].set_int(count);
  }
  
  if (rc == RC::RECORD_EOF) rc=RC::SUCCESS;
  result_tuple_.set_cells(result_cells);
  return rc;
}

RC AggregatePhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}

void AggregatePhysicalOperator::add_aggregation(const AggrOp aggregation)
{
  aggregations_.push_back(aggregation);
}
