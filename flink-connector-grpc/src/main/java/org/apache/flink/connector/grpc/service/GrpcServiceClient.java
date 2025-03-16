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

import com.google.common.util.concurrent.ListenableFuture;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.grpc.GrpcServiceOptions;
import org.apache.flink.table.data.RowData;

public interface GrpcServiceClient extends Closeable {

  ListenableFuture<RowData> asyncCall(RowData req, @Nullable Executor executor);

  static final SharedResourceHolder<SharedClientKey, GrpcServiceClient> SHARED_CLIENTS =
      new SharedResourceHolder<>(
          key ->
              new DeduplicatingClient(
                  new GrpcServiceClientImpl(
                      key.config(), key.requestSchema(), key.responseSchema())),
          GrpcServiceClient.class);

  public static GrpcServiceClient createClient(
      GrpcServiceOptions config,
      SerializationSchema<RowData> requestSchema,
      DeserializationSchema<RowData> responseSchema) {
    if (config.deduplicateRequests()) {
      final var key = new SharedClientKey(config, requestSchema, responseSchema);
      return SHARED_CLIENTS.createSharedResource(key);
    } else {
      return new GrpcServiceClientImpl(config, requestSchema, responseSchema);
    }
  }

  public record SharedClientKey(
      GrpcServiceOptions config,
      SerializationSchema<RowData> requestSchema,
      DeserializationSchema<RowData> responseSchema)
      implements Serializable {

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof SharedClientKey other) {
        // H4k!? : PbRowDataSerializationSchema does not implement equals. Use serialization to
        // compare.
        return Arrays.equals(toBytes(this), toBytes(other));
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(toBytes(this));
    }

    private static byte[] toBytes(Object o) {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
        oos.writeObject(o);
        oos.flush();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return bos.toByteArray();
    }
  }
}
