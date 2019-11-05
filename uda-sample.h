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

void MedInit(FunctionContext* context, StringVal* val);
void MedUpdate(FunctionContext* context, const DoubleVal& input, StringVal* val);
void MedMerge(FunctionContext* context, const StringVal& src, StringVal* dst);
StringVal MedSerialize(FunctionContext* context, const StringVal& val);
StringVal MedFinalize(FunctionContext* context, const StringVal& val);

// Utility function for serialization to StringVal
template <typename T>
StringVal ToStringVal(FunctionContext* context, const T& val);

#endif