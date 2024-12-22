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

import java.util.stream.Stream;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.types.DataType;

/**
 * Response handler for a base Lookup Join row which has the request and response as a single row.
 */
public interface CombinedRowResponseHandler extends GrpcResponseHandler<RowData, RowData, RowData> {

  static CombinedRowResponseHandler fromProjection(
      Projection reqProjection, Projection respProjection, DataType physicalRow) {
    // Create a data type that represents `new JoinedRowData(req, resp)`
    final DataType source = concatProjection(reqProjection, respProjection).project(physicalRow);

    // Create a projection from source -> physicalRow
    final int[][] successProjection =
        Projection.fromFieldNames(source, DataType.getFieldNames(physicalRow)).toNestedIndexes();

    final int respFieldCount = respProjection.toTopLevelIndexes().length;

    return (reqRow, respRow, error) ->
        ProjectedRowData.from(successProjection)
            .replaceRow(
                new JoinedRowData(
                    reqRow, respRow != null ? respRow : new GenericRowData(respFieldCount)));
  }

  // Utility method to create a joined Projection similar to JoinedRowData
  private static Projection concatProjection(Projection left, Projection right) {
    final int[][] indexes =
        Stream.of(left, right)
            .map(Projection::toNestedIndexes)
            .flatMap(Stream::of)
            .toArray(int[][]::new);
    return Projection.of(indexes);
  }
}
