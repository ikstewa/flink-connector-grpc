//
// Copyright 2024 Ian Stewart
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
package org.apache.flink.connector.grpc.service;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.grpc.GrpcServiceOptions;
import org.apache.flink.table.data.RowData;

public interface GrpcServiceClient extends Closeable {

  CompletableFuture<RowData> asyncCall(RowData req);

  public static GrpcServiceClient getOrCreate(
      GrpcServiceOptions config,
      SerializationSchema<RowData> requestSchema,
      DeserializationSchema<RowData> responseSchema) {
    return SharedGrpcServiceClient.getOrCreate(config, requestSchema, responseSchema);
  }
}
