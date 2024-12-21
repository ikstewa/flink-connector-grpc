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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.source.LookupTableSource.LookupContext;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

/**
 * Response handler for a base Lookup Join row which has the request and response as a single row.
 */
public record CombinedRowResponseHandler(
    DataType requestType,
    DataType responseType,
    GrpcResponseHandler<RowData, RowData, RowData> combiner)
    implements GrpcResponseHandler<RowData, RowData, RowData> {

  @Override
  public RowData handle(RowData request, RowData response, StatusRuntimeException err) {
    return combiner().handle(request, response, err);
  }

  public static CombinedRowResponseHandler fromLookupContext(
      DataType physicalRowDataType, LookupContext LookupContext) {
    return fromRowAndKeys(physicalRowDataType, LookupContext.getKeys());
  }

  /**
   * Given a DataType with only the physical columns and the lookup join keys, determine which
   * fields are mapped to the request type and the response type.
   *
   * <p>Note: Overlapping names take precendence from request to ensure join keys always match
   */
  private static CombinedRowResponseHandler fromRowAndKeys(DataType physicalRow, int[][] keys) {
    final List<String> fieldNames = LogicalTypeChecks.getFieldNames(physicalRow.getLogicalType());
    final List<DataType> fieldTypes = physicalRow.getChildren();

    final Set<Integer> keyIdx = new HashSet<>();
    for (int[] key : keys) {
      for (int keyIndex : key) {
        keyIdx.add(keyIndex);
      }
    }

    final List<Integer> reqRowIdx =
        IntStream.range(0, fieldNames.size()).filter(i -> keyIdx.contains(i)).boxed().toList();
    final List<Integer> respRowIdx =
        IntStream.range(0, fieldNames.size()).filter(i -> !keyIdx.contains(i)).boxed().toList();

    final DataType reqRowType =
        DataTypes.ROW(
            reqRowIdx.stream()
                .map(i -> DataTypes.FIELD(fieldNames.get(i), fieldTypes.get(i)))
                .toList());
    final DataType respRowType =
        DataTypes.ROW(
            respRowIdx.stream()
                .map(i -> DataTypes.FIELD(fieldNames.get(i), fieldTypes.get(i)))
                .toList());

    final GrpcResponseHandler<RowData, RowData, RowData> combiner =
        (reqRow, respRow, error) -> {
          GenericRowData row = new GenericRowData(fieldNames.size());
          if (respRow != null) {
            for (var i = 0; i < respRowIdx.size(); i++) {
              row.setField(respRowIdx.get(i), ((GenericRowData) respRow).getField(i));
            }
          }
          for (var i = 0; i < reqRowIdx.size(); i++) {
            row.setField(reqRowIdx.get(i), ((GenericRowData) reqRow).getField(i));
          }
          return row;
        };
    return new CombinedRowResponseHandler(reqRowType, respRowType, combiner);
  }
}
