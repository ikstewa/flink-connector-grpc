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
package org.apache.flink.connector.grpc;

import java.io.IOException;
import java.util.Collection;
import java.util.function.Function;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.grpc.handler.GrpcResponseHandler;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.util.FlinkRuntimeException;

public class GrpcLookupFunction extends LookupFunction {

  private final AsyncGrpcLookupFunction delegate;

  public GrpcLookupFunction(
      GrpcServiceOptions grpcConfig,
      Function<RowData, RowData> requestHandler,
      GrpcResponseHandler<RowData, RowData, RowData> responseHandler,
      SerializationSchema<RowData> requestSchema,
      DeserializationSchema<RowData> responseSchema) {
    this(
        new AsyncGrpcLookupFunction(
            grpcConfig, requestHandler, responseHandler, requestSchema, responseSchema, true));
  }

  private GrpcLookupFunction(AsyncGrpcLookupFunction delegate) {
    this.delegate = delegate;
  }

  @Override
  public void open(FunctionContext context) throws Exception {
    super.open(context);
    this.delegate.open(context);
  }

  @Override
  public void close() throws Exception {
    this.delegate.close();
  }

  @Override
  public Collection<RowData> lookup(RowData keyRow) throws IOException {
    try {
      return this.delegate.asyncLookup(keyRow).get();
    } catch (Exception e) {
      throw new FlinkRuntimeException("Error during lookup", e);
    }
  }
}
