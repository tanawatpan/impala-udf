// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <iostream>
#include <math.h>

#include <impala_udf/uda-test-harness.h>
#include "uda-sample.h"

using namespace impala;
using namespace impala_udf;
using namespace std;

bool TestCount() {
  // Use the UDA test harness to validate the COUNT UDA.
  UdaTestHarness<BigIntVal, BigIntVal, IntVal> test(
      CountInit, CountUpdate, CountMerge, NULL, CountFinalize);

  // Run the UDA over empty input
  vector<IntVal> empty;
  if (!test.Execute(empty, BigIntVal(0))) {
    cerr << "Count empty: " << test.GetErrorMsg() << endl;
    return false;
  }

  // Run the UDA over 10000 non-null values
  vector<IntVal> no_nulls;
  no_nulls.resize(10000);
  if (!test.Execute(no_nulls, BigIntVal(no_nulls.size()))) {
    cerr << "Count without nulls: " << test.GetErrorMsg() << endl;
    return false;
  }

  // Run the UDA with some nulls
  vector<IntVal> some_nulls;
  some_nulls.resize(10000);
  int expected = some_nulls.size();
  for (int i = 0; i < some_nulls.size(); i += 100) {
    some_nulls[i] = IntVal::null();
    --expected;
  }
  if (!test.Execute(some_nulls, BigIntVal(expected))) {
    cerr << "Count with nulls: " << test.GetErrorMsg() << endl;
    return false;
  }

  return true;
}

bool TestAvg() {
  typedef UdaTestHarness<StringVal, StringVal, DoubleVal> TestHarness;
  // Note: reinterpret_cast is required because pre-2.9 UDF headers had a spurious "const"
  // specifier in the return type for SerializeFn. It is unnecessary for 2.9+ headers.
  TestHarness test(AvgInit, AvgUpdate, AvgMerge,
      reinterpret_cast<TestHarness::SerializeFn>(AvgSerialize), AvgFinalize);
  test.SetIntermediateSize(16);

  vector<DoubleVal> vals;

  // Test empty input
  if (!test.Execute<DoubleVal>(vals, StringVal::null())) {
    cerr << "Avg empty: " << test.GetErrorMsg() << endl;
    return false;
  }

  // Test values
  for (int i = 0; i < 1001; ++i) {
    vals.push_back(DoubleVal(i));
  }

  cout << "TEST3";

  if (!test.Execute<DoubleVal>(vals, StringVal("1500"))) {
    cerr << "Avg: " << test.GetErrorMsg() << endl;
    return false;
  }
  return true;
}

int main(int argc, char** argv) {
  bool passed = true;
  passed &= TestCount();
  passed &= TestAvg();
  cout << "TEST4";
  cerr << (passed ? "Tests passed." : "Tests failed.") << endl;
  return 0;
}