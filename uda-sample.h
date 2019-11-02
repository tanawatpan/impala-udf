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


#ifndef SAMPLES_UDA_H
#define SAMPLES_UDA_H

#include <impala_udf/udf.h>

using namespace impala_udf;

// Note: As of Impala 1.2, UDAs must have the same intermediate and result types (see the
// udf.h header for the full Impala UDA specification, which can be found at
// https://github.com/cloudera/impala/blob/master/be/src/udf/udf.h). Some UDAs naturally
// conform to this limitation, such as Count and StringConcat. However, other UDAs return
// a numeric value but use a custom intermediate struct type that must be stored in a
// StringVal, such as Variance.
//
// As a workaround for now, these UDAs that require an intermediate buffer use StringVal
// for the intermediate and result type. In the UDAs' finalize functions, the numeric
// result is serialized to an ASCII string (see the ToStringVal() utility function
// provided with these samples). The returned StringVal is then cast back to the correct
// numeric type (see the Usage examples below).
//
// This restriction will be lifted in Impala 2.0.


// This is an example of the COUNT aggregate function.
//
// Usage: > create aggregate function my_count(int) returns bigint
//          location '/user/cloudera/libudasample.so' update_fn='CountUpdate';
//        > select my_count(col) from tbl;
void CountInit(FunctionContext* context, BigIntVal* val);
void CountUpdate(FunctionContext* context, const IntVal& input, BigIntVal* val);
void CountMerge(FunctionContext* context, const BigIntVal& src, BigIntVal* dst);
BigIntVal CountFinalize(FunctionContext* context, const BigIntVal& val);

// This is an example of the AVG(double) aggregate function. This function needs to
// maintain two pieces of state, the current sum and the count. We do this using
// the StringVal intermediate type. When this UDA is registered, it would specify
// 16 bytes (8 byte sum + 8 byte count) as the size for this buffer.
//
// Usage: > create aggregate function my_avg(double) returns string 
//          location '/user/cloudera/libudasample.so' update_fn='AvgUpdate';
//        > select cast(my_avg(col) as double) from tbl;
void AvgInit(FunctionContext* context, StringVal* val);
void AvgUpdate(FunctionContext* context, const DoubleVal& input, StringVal* val);
void AvgMerge(FunctionContext* context, const StringVal& src, StringVal* dst);
StringVal AvgSerialize(FunctionContext* context, const StringVal& val);
StringVal AvgFinalize(FunctionContext* context, const StringVal& val);

// Utility function for serialization to StringVal
template <typename T>
StringVal ToStringVal(FunctionContext* context, const T& val);

#endif
