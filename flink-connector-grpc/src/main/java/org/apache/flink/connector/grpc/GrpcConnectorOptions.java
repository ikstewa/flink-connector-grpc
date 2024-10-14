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

import static org.apache.flink.table.factories.FactoryUtil.FORMAT_SUFFIX;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Options for the GRPC connector. */
public final class GrpcConnectorOptions {

  private GrpcConnectorOptions() {}

  public static final ConfigOption<String> HOST =
      ConfigOptions.key("host").stringType().noDefaultValue().withDescription("GRPC hostname");

  public static final ConfigOption<Integer> PORT =
      ConfigOptions.key("port").intType().noDefaultValue().withDescription("GRPC host port");

  public static final ConfigOption<Boolean> PLAIN_TEXT =
      ConfigOptions.key("use-plain-text")
          .booleanType()
          .defaultValue(false)
          .withDescription("Determines whether to use plain text for the grpc service connection.");

  public static final ConfigOption<String> RPC_METHOD =
      ConfigOptions.key("grpc-method-name")
          .stringType()
          .noDefaultValue()
          .withDescription(
              "The fully qualified name of the GRPC method. "
                  + "See: io.grpc.MethodDescriptor.getFullMethodName() or io.grpc.stub.annotations.RpcMethod.fullMethodName()");

  // --------------------------------------------------------------------------------------------
  // Format options
  // --------------------------------------------------------------------------------------------

  public static final ConfigOption<String> REQUEST_FORMAT =
      ConfigOptions.key("request" + FORMAT_SUFFIX)
          .stringType()
          .noDefaultValue()
          .withDescription(
              "Defines the format identifier for encoding lookup grpc request object. "
                  + "The identifier is used to discover a suitable format factory.");

  public static final ConfigOption<String> RESPONSE_FORMAT =
      ConfigOptions.key("response" + FORMAT_SUFFIX)
          .stringType()
          .noDefaultValue()
          .withDescription(
              "Defines the format identifier for decoding grpc response object. "
                  + "The identifier is used to discover a suitable format factory.");
}
