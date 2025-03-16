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

import java.io.Serializable;
import java.util.List;

public record GrpcServiceOptions(
    String url,
    int port,
    boolean usePlainText,
    /**
     * The fully qualified name of the method used to build GRPC MethodDescriptor. @see
     * io.grpc.MethodDescriptor.getFullMethodName()
     */
    String serviceMethodName,
    int maxRetryTimes,
    long requestDeadlineMs,
    List<Integer> retryStatusCodes,
    List<Integer> errorStatusCodes,
    boolean deduplicateRequests)
    implements Serializable {

  // public GrpcServiceOptions(
  //     String url, int port, boolean usePlainText, MethodDescriptor<?, ?> grpc) {
  //   this(url, port, usePlainText, grpc.getFullMethodName());
  //   Preconditions.checkArgument(
  //       grpc.getType() == MethodType.UNARY, "Only support UNARY grpc service for lookup");
  // }
}
