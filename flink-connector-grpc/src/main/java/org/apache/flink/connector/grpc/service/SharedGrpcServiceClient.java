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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.grpc.GrpcServiceOptions;
import org.apache.flink.table.data.RowData;

class SharedGrpcServiceClient implements GrpcServiceClient {

  private static final ConcurrentHashMap<CacheKey, SharedRef> CLIENT_CACHE =
      new ConcurrentHashMap<>();

  public static GrpcServiceClient getOrCreate(
      GrpcServiceOptions config,
      SerializationSchema<RowData> requestSchema,
      DeserializationSchema<RowData> responseSchema) {
    final var key = new CacheKey(config, requestSchema, responseSchema);
    final var clientRef =
        CLIENT_CACHE.compute(
            key,
            (k, v) -> {
              if (v == null) {
                return new SharedRef(1, new SharedGrpcServiceClient(k));
              } else {
                return new SharedRef(v.count() + 1, v.client());
              }
            });
    return clientRef.client();
  }

  private final CacheKey key;
  private final GrpcServiceClient client;

  private SharedGrpcServiceClient(CacheKey key) {
    this.client =
        new GrpcServiceClientImpl(key.config(), key.requestSchema(), key.responseSchema());
    this.key = key;
  }

  @Override
  public CompletableFuture<RowData> asyncCall(RowData req) {
    return this.client.asyncCall(req);
  }

  @Override
  public void close() throws IOException {
    final var clientRef =
        CLIENT_CACHE.compute(
            key,
            (k, v) -> {
              if (v == null) {
                return null; // Should never happen unless bug
              } else if (v.count() == 1) {
                return null;
              } else {
                return new SharedRef(v.count() - 1, v.client());
              }
            });
    if (clientRef == null) {
      this.client.close();
    }
  }

  private record SharedRef(int count, SharedGrpcServiceClient client) {}

  private record CacheKey(
      GrpcServiceOptions config,
      SerializationSchema<RowData> requestSchema,
      DeserializationSchema<RowData> responseSchema)
      implements Serializable {

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof CacheKey other) {
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
