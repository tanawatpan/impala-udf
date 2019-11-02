#include <iostream>
#include <math.h>

#include <impala_udf/uda-test-harness.h>
#include "udaf_median.h"

using namespace impala;
using namespace impala_udf;
using namespace std;

bool TestMed() {
  typedef UdaTestHarness<StringVal, StringVal, DoubleVal> TestHarness;
  // Note: reinterpret_cast is required because pre-2.9 UDF headers had a spurious "const"
  // specifier in the return type for SerializeFn. It is unnecessary for 2.9+ headers.
  TestHarness test(MedInit, MedUpdate, MedMerge,
      reinterpret_cast<TestHarness::SerializeFn>(MedSerialize), MedFinalize);
  test.SetIntermediateSize(16);

  vector<DoubleVal> vals;

  // Test empty input
  if (!test.Execute<DoubleVal>(vals, StringVal::null())) {
    cerr << "Med empty: " << test.GetErrorMsg() << endl;
    return false;
  }

  // Test values
  for (int i = 0; i < 1001; ++i) {
    vals.push_back(DoubleVal(i));
  }
  if (!test.Execute<DoubleVal>(vals, StringVal("500"))) {
    cerr << "Med: " << test.GetErrorMsg() << endl;
    return false;
  }
  return true;
}

int main(int argc, char** argv) {
  bool passed = true;
  passed &= TestMed();
  cerr << (passed ? "Tests passed." : "Tests failed.") << endl;
  return 0;
}