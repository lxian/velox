//
// Created by 冼立 on 2023/6/26.
//

#include <iostream>
#include <algorithm>
#include <vector>
#include "velox/expression/Expr.h"
#include "velox/buffer/Buffer.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/FlatVector.h"
#include <velox/core/Expressions.h>
#include <velox/core/ITypedExpr.h>
#include <velox/core/PlanFragment.h>
#include <velox/core/PlanNode.h>
#include <velox/exec/Task.h>
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

using namespace facebook::velox;

const TypePtr& BIGINT_TYPE = createScalarType(TypeKind::BIGINT);
const TypePtr& VARCHAR_TYPE = createScalarType(TypeKind::VARCHAR);

std::shared_ptr<memory::MemoryPool> rootPool{
    memory::defaultMemoryManager().addRootPool()};
std::shared_ptr<memory::MemoryPool> pool{rootPool->addLeafChild("leaf")};


template <typename VI , typename VO, class UnaryOperation>
std::vector<VO> map(std::vector<VI> ins, UnaryOperation op) {
  std::vector<VO> ous;
  ous.reserve(ins.size());
  for (auto& item : ins) {
    ous.push_back(op(item));
  }
  return ous;
}

template <typename Ele>
std::vector<Ele> vec(std::initializer_list<Ele> eles) {
  std::vector<Ele> elev;
  for (const auto& item : eles) {
    elev.push_back(item);
  }
  return elev;
}

core::FieldAccessTypedExprPtr colRef(const std::string& col, const TypePtr& type = BIGINT_TYPE) {
  return std::make_shared<core::FieldAccessTypedExpr>(type, col);
}

core::CallTypedExprPtr call(const std::string& func, const std::vector<std::string>& cols, const TypePtr& type = BIGINT_TYPE) {
  std::vector<core::TypedExprPtr> ins = map<std::string, core::TypedExprPtr>(
      cols, [type](std::string& col) { return colRef(col, type); });
  return std::make_shared<core::CallTypedExpr>(type, ins, func);
}

VectorPtr makeStringDict(std::vector<std::string> values, std::vector<int> idxes) {
  auto strings =  BaseVector::create<FlatVector<StringView>>(VARCHAR_TYPE, values.size(), pool.get());
  for (int i = 0; i < values.size(); ++i) {
    strings->set(i, values[i].data());
  }
  BufferPtr indices = AlignedBuffer::allocate<vector_size_t>(idxes.size(), pool.get());
  auto rawValues = indices->asMutable<vector_size_t>();
  for (int i = 0; i < indices->size(); ++i) {
    *(rawValues + i) = idxes[i];
  }

  const SelectivityVector rows(idxes.size(), true);
  return BaseVector::wrapInDictionary(nullptr, indices, idxes.size(), strings);
}

VectorPtr makeBigIntFlatDict(std::vector<int> values) {
  auto vector = BaseVector::create<FlatVector<int64_t>>(BIGINT_TYPE, values.size(), pool.get());
  for (int i = 0; i < 10; ++i) {
    vector->set(i, values[i]);
  }
  return vector;
}

void simpleExecutePlanNode(const core::PlanNodePtr planNode) {
  core::PlanFragment planFragment{planNode};
  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(
      std::thread::hardware_concurrency());
  auto queryCtx = std::make_shared<core::QueryCtx>(executor.get());
  auto task = exec::Task::create("task0", std::move(planFragment), 0, std::move(queryCtx));
  RowVectorPtr ret;
  for(;;) {
    ret = task->next();
    if (ret) {
      std::cout << ">>>Ret" << std::endl;
      std::cout << ret->toString(0,10 ) << std::endl;
    } else {
      std::cout << "<<<End" << std::endl;
      break;
    }
  }
}


void tryTask() {

  BufferPtr values = AlignedBuffer::allocate<int64_t>(100, pool.get());
  int64_t* rawValues = values->asMutable<int64_t>();
  *rawValues = 100;
  *(rawValues + 1) = 101;
  const int64_t* readonlyRawValues = values->as<int64_t>();
  std::cout << *readonlyRawValues << std::endl;
  std::cout << *(readonlyRawValues + 1) << std::endl;

  // vec
  const TypePtr& type = BIGINT_TYPE;
  auto vector =  BaseVector::create<FlatVector<int64_t>>(type, 10, pool.get());
  for (int i = 0; i < 10; ++i) {
    vector->set(i, i % 3);
  }
  auto vector2 =  BaseVector::create<FlatVector<int64_t>>(type, 10, pool.get());
  for (int i = 0; i < 10; ++i) {
    vector2->set(i, 123);
  }

  // row
  auto rowType = ROW({"col1", "col2"}, {type, type});
  std::vector<VectorPtr> cols; cols.push_back(vector); cols.push_back(vector2);
  auto row = std::make_shared<RowVector>(pool.get(), rowType, nullptr, 10, std::move(cols));
  std::cout << row->toString(0, 10) << std::endl;
  //  rowV->append(vector.get());

  // values node
  auto valuesNode = std::make_shared<core::ValuesNode>(
      "values0", std::vector<RowVectorPtr>{row});

  // project node
  auto projectNode = std::make_shared<core::ProjectNode>(
      "project0",
      //      std::vector<std::string>{"col1", "col2"},
      //      std::vector<core::TypedExprPtr>{colRef("col1"), colRef("col2")},
      std::vector<std::string>{"add", "sub"},
      std::vector<core::TypedExprPtr>{
          call("plus", {"col2", "col1"}),
          call("minus", {"col2", "col1"}),
      },
      valuesNode);

  auto projectNode1 = std::make_shared<core::ProjectNode>(
      "project1",
      std::vector<std::string>{"ret"},
      std::vector<core::TypedExprPtr>{call("multiply", {"add", "sub"})},
      projectNode);

  core::PlanFragment planFragment{projectNode1};
  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(
      std::thread::hardware_concurrency());
  auto queryCtx = std::make_shared<core::QueryCtx>(executor.get());
  auto task = exec::Task::create("task0", std::move(planFragment), 0, std::move(queryCtx));
  RowVectorPtr ret;
  for(;;) {
    ret = task->next();
    if (ret) {
      std::cout << ">>>Ret" << std::endl;
      std::cout << ret->toString(0,10 ) << std::endl;
    } else {
      std::cout << "<<<End" << std::endl;
      break;
    }
  }

  //  // agg node
  //  std::vector<core::TypedExprPtr> aggInputs{std::make_shared<core::FieldAccessTypedExpr>(type, "col2")};
  //  auto callExpr = std::make_shared<core::CallTypedExpr>(createScalarType(TypeKind::BIGINT), aggInputs, "sum");
  //  auto aggr = core::AggregationNode::Aggregate{callExpr};
  //  auto aggNode = std::make_shared<core::AggregationNode>(
  //      "agg0",
  //      core::AggregationNode::Step::kFinal,
  //      std::vector<core::FieldAccessTypedExprPtr>{std::make_shared<core::FieldAccessTypedExpr>(type, "col1")},
  //      std::vector<core::FieldAccessTypedExprPtr>{},
  //      std::vector<std::string>{"agg1"},
  //      std::vector<core::AggregationNode::Aggregate>{aggr}, true, projectNode);
  //
  //  // task
  //  core::PlanFragment planFragment{aggNode};
  //  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(
  //      std::thread::hardware_concurrency());
  //  auto queryCtx = std::make_shared<core::QueryCtx>(executor.get());
  //  auto task = exec::Task::create("task0", std::move(planFragment), 0, std::move(queryCtx));
  //  auto ret = task->next();
  //  std::cout << ">>>Ret" << std::endl;
  //  std::cout << ret->toString(0,10 ) << std::endl;
}

void simpleEval(VectorPtr in, const std::string& funcName) {
  const SelectivityVector rows(in->size(), true);
  std::vector<VectorPtr> cols; cols.push_back(in);
  auto rowType = ROW({"col1"}, {in->type()});
  auto row = std::make_shared<RowVector>(pool.get(), rowType, nullptr, 10, std::move(cols));

  auto funcCall = call(funcName, {"col1"}, in->type());
  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(
      std::thread::hardware_concurrency());
  auto queryCtx = std::make_shared<core::QueryCtx>(executor.get());
  exec::SimpleExpressionEvaluator evaluator(queryCtx.get(), pool.get());
  auto exprSet = evaluator.compile(funcCall);
  VectorPtr ret;
  evaluator.evaluate(exprSet.get(), rows, *row.get(), ret);
  std::cout << ret->toString(0, 10) << std::endl;
}

void tryDic() {
  auto dictVec = makeStringDict({"A0", "B1", "C2"}, {0,1,2,0,1,2,0,1,2,0,1});

  auto simple = dictVec->as<SimpleVector<StringView>>();
  std::cout << ">>>SIMPLE" << std::endl;
  for (int i = 0; i < 10; i++) {
    std::cout << i << ": " << simple->valueAt(i) << std::endl;
  }

  simpleEval(dictVec, "lower");
}

void tryAgg() {

  // row
  auto rowType = ROW({"strCol", "intCol"}, {VARCHAR_TYPE, BIGINT_TYPE});
  auto row = std::make_shared<RowVector>(pool.get(), rowType, nullptr, 10,
                                         std::vector<VectorPtr>{
                                             makeStringDict({"A0", "B1", "C2"}, {0,1,2,0,1,2,0,1,2,0,1}),
                                             makeBigIntFlatDict({                      1,2,3,4,5,6,7,8,9,9,9})
                                         });

  // values node
  auto valuesNode0 = std::make_shared<core::ValuesNode>(
      "values0", std::vector<RowVectorPtr>{row});

//  auto projectNode0 = std::make_shared<core::ProjectNode>(
//      "project0",
//      std::vector<std::string>{"strCol", "intCol"},
//      std::vector<core::TypedExprPtr>{colRef("strCol"), colRef("intCol")},
//      valuesNode0);

  auto aggNode0 = std::make_shared<core::AggregationNode>(
      "agg0", core::AggregationNode::Step::kSingle,
      std::vector<core::FieldAccessTypedExprPtr>{colRef("strCol", VARCHAR_TYPE)},
      std::vector<core::FieldAccessTypedExprPtr>{},
      std::vector<std::string>{"sum(intCol)"},
      std::vector<core::AggregationNode::Aggregate>{core::AggregationNode::Aggregate{call("sum", {"intCol"}, BIGINT_TYPE), nullptr}},
      true, valuesNode0);

  simpleExecutePlanNode(aggNode0);

}


int main(int argc, char** argv) {
  aggregate::prestosql::registerAllAggregateFunctions();
  functions::prestosql::registerAllScalarFunctions();

//  tryTask();
//  tryDic();
  tryAgg();
}