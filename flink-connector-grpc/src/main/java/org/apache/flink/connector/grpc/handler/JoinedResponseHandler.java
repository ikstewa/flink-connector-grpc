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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;

/** Response handler chains two response handlers and returns a joined row result. */
public record JoinedResponseHandler<ReqT, RespT>(
    GrpcResponseHandler<ReqT, RespT, RowData> left, GrpcResponseHandler<ReqT, RespT, RowData> right)
    implements GrpcResponseHandler<ReqT, RespT, RowData> {

  @Override
  public RowData handle(ReqT request, RespT response, StatusRuntimeException error) {
    return new JoinedRowData(
        left.handle(request, response, error), right.handle(request, response, error));
  }
}
