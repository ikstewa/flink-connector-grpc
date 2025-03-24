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
package org.apache.flink.connector.grpc.handler;

import io.grpc.StatusRuntimeException;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.flink.util.FlinkRuntimeException;

/** Delegating response handler that throws on a configurable set of status codes. */
public record ErrorResponseHandler<ReqT, RespT, ProducedT>(
    Set<Integer> errorCodes, GrpcResponseHandler<ReqT, RespT, ProducedT> handler)
    implements GrpcResponseHandler<ReqT, RespT, ProducedT> {

  @Override
  public ProducedT handle(
      ReqT request, @Nullable RespT response, @Nullable StatusRuntimeException error) {
    if (error != null && errorCodes().contains(error.getStatus().getCode().value())) {
      // Throw a new exception here as the Status in StatusRuntimeException is not serializeable
      throw new FlinkRuntimeException("GRPC request failed after retries: " + error.getMessage());
    } else {
      return handler.handle(request, response, error);
    }
  }
}
