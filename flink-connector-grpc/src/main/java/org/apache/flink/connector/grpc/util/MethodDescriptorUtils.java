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
package org.apache.flink.connector.grpc.util;

import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.ReflectableMarshaller;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.protobuf.PbDecodingFormat;
import org.apache.flink.formats.protobuf.PbEncodingFormat;
import org.apache.flink.formats.protobuf.PbFormatConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;

public final class MethodDescriptorUtils {

  private MethodDescriptorUtils() {}

  public static <ReqT, RespT> FlinkMethodDescriptor fromMethodDescriptor(
      MethodDescriptor<ReqT, RespT> desc) throws IllegalArgumentException {

    if (!desc.getType().equals(MethodDescriptor.MethodType.UNARY)) {
      throw new IllegalArgumentException("Only UNARY method types supported.");
    }

    final var respDecoder =
        new PbDecodingFormat(
            new PbFormatConfig.PbFormatConfigBuilder()
                .messageClassName(
                    ((ReflectableMarshaller<RespT>) desc.getResponseMarshaller())
                        .getMessageClass()
                        .getName())
                .build());
    final var reqEncoder =
        new PbEncodingFormat(
            new PbFormatConfig.PbFormatConfigBuilder()
                .messageClassName(
                    ((ReflectableMarshaller<ReqT>) desc.getRequestMarshaller())
                        .getMessageClass()
                        .getName())
                .build());
    return new FlinkMethodDescriptor(desc.getFullMethodName(), respDecoder, reqEncoder);
  }

  public static <ReqT, RespT> FlinkMethodDescriptor parseMethodDescriptor(String fqMethod)
      throws IllegalArgumentException {
    return fromMethodDescriptor(loadMethodDescriptor(fqMethod));
  }

  @SuppressWarnings("unchecked")
  public static <ReqT, RespT> io.grpc.MethodDescriptor<ReqT, RespT> loadMethodDescriptor(
      String method) {

    try {
      final var clazzName = method.split("#")[0];
      final var methodName = method.split("#")[1];

      Class<?> grpcClass =
          Class.forName(clazzName, true, Thread.currentThread().getContextClassLoader());
      return (io.grpc.MethodDescriptor<ReqT, RespT>) grpcClass.getMethod(methodName).invoke(null);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format("Failed to load method descriptor from %s", method), e);
    }
  }

  public record FlinkMethodDescriptor(
      String serviceMethodName,
      DecodingFormat<DeserializationSchema<RowData>> responseDecoder,
      EncodingFormat<SerializationSchema<RowData>> requestEncoder) {}
}
