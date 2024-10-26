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

import com.google.common.truth.Truth;
import io.grpc.examples.helloworld.GreeterGrpc;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MethodDescriptorUtilsTest {

  @Test
  @DisplayName("Can load test service")
  void test_service() {

    final var desc =
        MethodDescriptorUtils.parseMethodDescriptor(
            "io.grpc.examples.helloworld.GreeterGrpc#getSayHelloMethod");

    Truth.assertThat(desc.serviceMethodName())
        .isEqualTo(GreeterGrpc.getSayHelloMethod().getFullMethodName());
  }

  @Test
  @DisplayName("Fails on unkown service")
  void test_service_fails() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            MethodDescriptorUtils.parseMethodDescriptor("io.grpc.examples.helloworld.GreeterGrpc"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            MethodDescriptorUtils.parseMethodDescriptor(
                "io.grpc.examples.helloworld.GreeterGrpcNoExist#getSayHelloMethod"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            MethodDescriptorUtils.parseMethodDescriptor(
                "io.grpc.examples.helloworld.GreeterGrpc#doesNotExist"));
  }
}
