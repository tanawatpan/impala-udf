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

#include "uda-sample.h"
#include <algorithm>
#include <sstream>
#include <iostream>
#include <assert.h>
#include <list>

using namespace impala_udf;
using namespace std;

template <typename T>
StringVal ToStringVal(FunctionContext *context, const T &val)
{
  stringstream ss;
  ss << val;
  string str = ss.str();
  StringVal string_val(context, str.size());
  memcpy(string_val.ptr, str.c_str(), str.size());
  return string_val;
}

template <>
StringVal ToStringVal<DoubleVal>(FunctionContext *context, const DoubleVal &val)
{
  if (val.is_null)
    return StringVal::null();
  return ToStringVal(context, val.val);
}

struct t_median
{
  list<double> *values;
  int64_t count;
};

// Initialize the StringVal intermediate to a zero'd MedStruct
void MedInit(FunctionContext *context, StringVal *val)
{
  val->ptr = context->Allocate(sizeof(t_median));
  // Exit on failed allocation. Impala will fail the query after some time.
  if (val->ptr == NULL)
  {
    *val = StringVal::null();
    return;
  }
  val->is_null = false;
  val->len = sizeof(t_median);
  memset(val->ptr, 0, val->len);
}

void MedUpdate(FunctionContext *context, const DoubleVal &input, StringVal *val)
{
  if (input.is_null)
    return;
  // Handle failed allocation. Impala will fail the query after some time.
  if (val->is_null)
    return;

  assert(val->len == sizeof(t_median));

  t_median *median = reinterpret_cast<t_median *>(val->ptr);

  if (median->values == NULL)
    median->values = new std::list<double>();

  median->values->push_back(input.val);
  median->count += 1;
}

void MedMerge(FunctionContext *context, const StringVal &src, StringVal *dst)
{
  if (src.is_null || dst->is_null)
    return;

  const t_median *src_median = reinterpret_cast<t_median *>(src.ptr);
  t_median *dst_median = reinterpret_cast<t_median *>(dst->ptr);

  if (dst_median->values == NULL)
    dst_median->values = new std::list<double>();

  if (src_median->values != NULL)
    dst_median->values->merge(*src_median->values);

  dst_median->count += src_median->count;
}

// A serialize function is necesary to free the intermediate state allocation. We use the
// StringVal constructor to allocate memory owned by Impala, copy the intermediate state,
// and free the original allocation. Note that memory allocated by the StringVal ctor is
// not necessarily persisted across UDA function calls, which is why we don't use it in
// MedInit().
StringVal MedSerialize(FunctionContext *context, const StringVal &val)
{
  if (val.is_null)
    return StringVal::null();
  // Copy the value into Impala-managed memory with StringVal::CopyFrom().
  // NB: CopyFrom() will return a null StringVal and and fail the query if the allocation
  // fails because of lack of memory.
  StringVal result = StringVal::CopyFrom(context, val.ptr, val.len);
  context->Free(val.ptr);
  return result;
}

StringVal MedFinalize(FunctionContext *context, const StringVal &val)
{
  if (val.is_null)
    return StringVal::null();

  assert(val.len == sizeof(t_median));

  t_median *median = reinterpret_cast<t_median *>(val.ptr);

  if (median->values == NULL || median->values->empty())
  {
    // Free Memory
    context->Free(val.ptr);
    return StringVal::null();
  }

  median->values->sort();

  list<double>::iterator it = median->values->begin();
  // Move iterator to middle position.
  advance(it, median->count / 2);

  StringVal result;

  if (median->count % 2 == 1)
  {
    result = ToStringVal(context, *it);
  }
  else
  {
    double right = *it;
    advance(it, -1);
    double left = *it;
    result = ToStringVal(context, (left + right) / 2);
  }

  // Free Memory
  delete (median->values);
  context->Free(val.ptr);

  return result;
}
