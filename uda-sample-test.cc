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
#include <cstdio>
#include <ctime>

#include <impala_udf/uda-test-harness.h>
#include "uda-sample.h"

using namespace impala;
using namespace impala_udf;
using namespace std;

bool TestMed()
{
  typedef UdaTestHarness<StringVal, StringVal, DoubleVal> TestHarness;
  // Note: reinterpret_cast is required because pre-2.9 UDF headers had a spurious "const"
  // specifier in the return type for SerializeFn. It is unnecessary for 2.9+ headers.
  TestHarness test(MedInit, MedUpdate, MedMerge,
                   reinterpret_cast<TestHarness::SerializeFn>(MedSerialize), MedFinalize);
  test.SetIntermediateSize(16);

  // Test empty input
  vector<DoubleVal> val_null;
  if (!test.Execute<DoubleVal>(val_null, StringVal::null()))
  {
    cerr << "Med empty: " << test.GetErrorMsg() << endl;
    return false;
  }

  // Test some values is null
  vector<DoubleVal> some_nulls;
  for (int i = 0; i < 155; ++i)
  {
    some_nulls.push_back(DoubleVal(i));
    some_nulls.push_back(DoubleVal::null());
  }

  if (!test.Execute<DoubleVal>(some_nulls, StringVal("77")))
  {
    cerr << "Med: " << test.GetErrorMsg() << endl;
    return false;
  }

  // Test large values
  vector<DoubleVal> large_vals;
  for (int i = 0; i < 100001; ++i)
  {
    large_vals.push_back(DoubleVal(i));
  }

  if (!test.Execute<DoubleVal>(large_vals, StringVal("50000")))
  {
    cerr << "Med: " << test.GetErrorMsg() << endl;
    return false;
  }

  // Test small values
  vector<DoubleVal> small_vals;
  for (int i = 0; i < 4; ++i)
  {
    small_vals.push_back(DoubleVal(i));
  }

  if (!test.Execute<DoubleVal>(small_vals, StringVal("1.5")))
  {
    cerr << "Med: " << test.GetErrorMsg() << endl;
    return false;
  }

  return true;
}

int main(int argc, char **argv)
{
  std::clock_t start;
  double duration;

  bool passed = true;

  start = std::clock();

  passed &= TestMed();

  cerr << (passed ? "Tests passed." : "Tests failed.") << endl;

  duration = (std::clock() - start) / (double)CLOCKS_PER_SEC;
  std::cout << "Times: " << duration << '\n';

  return 0;
}