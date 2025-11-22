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

import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.jspecify.annotations.Nullable;

/** Response handler that amends metadata fields to the end of a delegate response handler */
public record GrpcMetadataParser(List<GrpcMetadataField> metaFields) implements Serializable {

  private static final MapData EMPTY_MAP = new GenericMapData(Map.of());

  public RowData toRow(@Nullable StatusRuntimeException error) {
    final var metadataRow = new GenericRowData(metaFields.size());
    for (var i = 0; i < metaFields.size(); i++) {
      final var metaVal =
          switch (metaFields.get(i)) {
            case STATUS_CODE ->
                Optional.ofNullable(error).map(e -> e.getStatus().getCode().value()).orElse(0);
            case STATUS_DESCRIPTION ->
                Optional.ofNullable(error)
                    .map(e -> StringData.fromString(error.getStatus().getDescription()))
                    .orElse(null);
            case STATUS_TRAILERS ->
                Optional.ofNullable(error)
                    .map(e -> toStringMapData(e.getTrailers()))
                    .orElse(EMPTY_MAP);
            case STATUS_TRAILERS_BINARY ->
                Optional.ofNullable(error)
                    .map(e -> toBinaryMapData(e.getTrailers()))
                    .orElse(EMPTY_MAP);
            case RESPONSE_TIME -> System.currentTimeMillis();
          };
      metadataRow.setField(i, metaVal);
    }
    return metadataRow;
  }

  private static MapData toStringMapData(Metadata meta) {
    return new GenericMapData(
        meta.keys().stream()
            .filter(k -> !k.endsWith(Metadata.BINARY_HEADER_SUFFIX))
            .map(k -> Metadata.Key.of(k, io.grpc.Metadata.ASCII_STRING_MARSHALLER))
            .collect(
                Collectors.toMap(
                    k -> StringData.fromString(k.name()),
                    k -> StringData.fromString(meta.get(k)))));
  }

  private static MapData toBinaryMapData(Metadata meta) {
    return new GenericMapData(
        meta.keys().stream()
            .filter(k -> k.endsWith(Metadata.BINARY_HEADER_SUFFIX))
            .map(k -> Metadata.Key.of(k, io.grpc.Metadata.BINARY_BYTE_MARSHALLER))
            .collect(
                Collectors.toMap(k -> StringData.fromString(k.name()), k -> (byte[]) meta.get(k))));
  }
}
