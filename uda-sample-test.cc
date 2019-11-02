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

  if (!test.Execute<DoubleVal>(vals, StringVal("500"))) {
    cerr << "Avg: " << test.GetErrorMsg() << endl;
    return false;
  }
  return true;
}

int main(int argc, char** argv) {
  bool passed = true;
  passed &= TestAvg();
  cerr << (passed ? "Tests passed." : "Tests failed.") << endl;
  return 0;
}