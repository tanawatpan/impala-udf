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