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

import java.util.stream.Stream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

public enum GrpcMetadataField {
  STATUS_CODE("status-code", DataTypes.INT().notNull()),
  STATUS_DESCRIPTION("status-description", DataTypes.STRING().nullable()),
  STATUS_TRAILERS(
      "status-trailers",
      DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.STRING().nullable()).notNull()),
  STATUS_TRAILERS_BINARY(
      "status-trailers-bin",
      DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.BYTES().nullable()).notNull()),
  RESPONSE_TIME("response-time", DataTypes.BIGINT().notNull());

  final String key;

  final DataType dataType;

  GrpcMetadataField(String key, DataType dataType) {
    this.key = key;
    this.dataType = dataType;
  }

  public static GrpcMetadataField fromKey(String val) {
    return Stream.of(GrpcMetadataField.values())
        .filter(md -> md.key.equals(val))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("Invalid metadata field: " + val));
  }
}
