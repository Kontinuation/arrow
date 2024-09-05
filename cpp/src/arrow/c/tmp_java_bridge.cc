// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <iostream>
#include "arrow/api.h"
#include "arrow/array/builder_dict.h"
#include "arrow/c/bridge.h"
#include "arrow/c/com_wherobots_CDataToJavaCppExample.h"

extern "C" JNIEXPORT void JNICALL
Java_com_wherobots_CDataToJavaCppExample_FillInt64Array(
    JNIEnv*, jobject, jlong c_schema_ptr, jlong c_array_ptr) {
  // arrow::Int64Builder builder;
  // builder.Append(1);
  // builder.Append(2);
  // builder.Append(3);
  // builder.AppendNull();
  // builder.Append(5);
  // builder.Append(6);
  // builder.Append(7);
  // builder.Append(8);
  // builder.Append(9);
  // builder.Append(10);
  // std::shared_ptr<arrow::Array> array = *builder.Finish();

  // struct ArrowSchema* c_schema = reinterpret_cast<struct ArrowSchema*>(c_schema_ptr);
  // auto c_schema_status = arrow::ExportType(*array->type(), c_schema);
  // if (!c_schema_status.ok()) c_schema_status.Abort();

  // struct ArrowArray* c_array = reinterpret_cast<struct ArrowArray*>(c_array_ptr);
  // auto c_array_status = arrow::ExportArray(*array, c_array);
  // if (!c_array_status.ok()) c_array_status.Abort();

  
  // Define the dictionary values
  arrow::StringBuilder dict_builder;
  dict_builder.Append("apple");
  dict_builder.Append("banana");
  dict_builder.Append("cherry");

  std::shared_ptr<arrow::Array> dict_array;
  dict_builder.Finish(&dict_array);

  // Create the dictionary type
  auto dict_type = arrow::dictionary(arrow::int32(), arrow::utf8());

  // Define the indices for the dictionary-encoded array
  arrow::Int32Builder indices_builder;
  indices_builder.Append(0);
  indices_builder.Append(1);
  indices_builder.Append(0);
  indices_builder.Append(2);
  indices_builder.Append(1);
  indices_builder.Append(0);
  indices_builder.Append(1);
  indices_builder.Append(0);
  indices_builder.Append(2);
  indices_builder.Append(1);
  indices_builder.Append(0);
  indices_builder.Append(1);
  indices_builder.Append(0);
  indices_builder.Append(2);
  indices_builder.Append(1);  

  std::shared_ptr<arrow::Array> indices_array;
  indices_builder.Finish(&indices_array);

  // Create the dictionary-encoded array
  auto dict_encoded_array = std::make_shared<arrow::DictionaryArray>(dict_type, indices_array, dict_array);

  // Print the dictionary-encoded array
  std::cout << "Dictionary-encoded array: " << dict_encoded_array->ToString() << std::endl;

  auto c_schema = reinterpret_cast<struct ArrowSchema*>(c_schema_ptr);
  auto c_schema_status = arrow::ExportType(*dict_encoded_array->type(), c_schema);
  if (!c_schema_status.ok()) c_schema_status.Abort();

  auto c_array = reinterpret_cast<struct ArrowArray*>(c_array_ptr);
  auto c_array_status = arrow::ExportArray(*dict_encoded_array, c_array);
  if (!c_array_status.ok()) c_array_status.Abort();
}
