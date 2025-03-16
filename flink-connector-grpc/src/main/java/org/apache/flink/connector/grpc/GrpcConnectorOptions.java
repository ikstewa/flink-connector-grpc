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

import java.util.List;
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

  public static final ConfigOption<String> RPC_METHOD_DESC =
      ConfigOptions.key("grpc-method-desc")
          .stringType()
          .noDefaultValue()
          .withDescription(
              "Fully qualified reference to a static method for retrieving a MethodDescriptor. "
                  + " Example: 'io.grpc.examples.helloworld.GreeterGrpc#getSayHelloMethod'.");

  // --------------------------------------------------------------------------------------------
  // GRPC Status code options
  // --------------------------------------------------------------------------------------------
  //  Retry
  //    CANCELLED	1
  //    UNKNOWN	2
  //    DEADLINE_EXCEEDED	4
  //    RESOURCE_EXHAUSTED	8
  //    ABORTED	10
  //    INTERNAL	13
  //    UNAVAILABLE	14
  //  Error
  //    INVALID_ARGUMENT	3
  //    ALREADY_EXISTS	6
  //    PERMISSION_DENIED	7
  //    FAILED_PRECONDITION	9
  //    OUT_OF_RANGE	11
  //    UNIMPLEMENTED	12
  //    DATA_LOSS	15
  //    UNAUTHENTICATED	16
  //  Other
  //    OK	0
  //    NOT_FOUND	5

  public static final ConfigOption<List<Integer>> GRPC_RETRY_CODES =
      ConfigOptions.key("grpc-retry-codes")
          .intType()
          .asList()
          .defaultValues(1, 2, 4, 8, 10, 13, 14)
          .withDescription(
              "List of GRPC status codes that should be retried, separated by semicolon.");
  public static final ConfigOption<List<Integer>> GRPC_ERROR_CODES =
      ConfigOptions.key("grpc-error-codes")
          .intType()
          .asList()
          .defaultValues(3, 6, 7, 9, 11, 12, 15, 16)
          .withDescription(
              "List of GRPC status codes that should be treated as errors, separated by semicolon.");
  public static final ConfigOption<Boolean> GRPC_DEDUPLICATE_REQUESTS =
      ConfigOptions.key("grpc-deduplicate-requests")
          .booleanType()
          .defaultValue(true)
          .withDescription(
              "Concurrent requests for the same lookup key are deduplicated and only a single request is set to origin.");
  public static final ConfigOption<Long> GRPC_REQUEST_TIMEOUT =
      ConfigOptions.key("grpc-request-timeout-ms")
          .longType()
          .defaultValue(-1L)
          .withDescription(
              "Optional duration in ms to apply a deadline for the GRPC request. If values is < 0 no deadline is applied.");
  public static final ConfigOption<Boolean> ASYNC =
      ConfigOptions.key("async")
          .booleanType()
          .defaultValue(true)
          .withDescription("Whether to use an async lookup join function");

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
