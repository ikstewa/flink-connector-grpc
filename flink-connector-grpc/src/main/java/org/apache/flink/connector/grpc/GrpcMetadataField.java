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
  STATUS_CODE("status_code", DataTypes.INT().notNull()),
  STATUS_DESCRIPTION("status_description", DataTypes.STRING().nullable());
  // HEADERS(
  //         "headers",
  //         // key and value of the map are nullable to make handling easier in queries
  //         DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.BYTES().nullable())
  //                 .notNull(),
  //         new MetadataConverter() {
  //             private static final long serialVersionUID = 1L;
  //
  //             @Override
  //             public Object read(ConsumerRecord<?, ?> record) {
  //                 final Map<StringData, byte[]> map = new HashMap<>();
  //                 for (Header header : record.headers()) {
  //                     map.put(StringData.fromString(header.key()), header.value());
  //                 }
  //                 return new GenericMapData(map);
  //             }
  //         }),

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
