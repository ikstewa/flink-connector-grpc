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

import java.util.stream.Stream;
import org.apache.flink.table.connector.Projection;

public final class Projections {

  private Projections() {}

  // Utility method to create a joined Projection similar to JoinedRowData
  public static Projection concat(int[][] left, int[][] right) {
    final int[][] indexes = Stream.of(left, right).flatMap(Stream::of).toArray(int[][]::new);
    return Projection.of(indexes);
  }

  // Removes all fields which exceed the max field count
  public static Projection trim(Projection proj, int maxFields) {
    final int[][] indexes =
        Stream.of(proj.toNestedIndexes()).filter(idx -> idx[0] < maxFields).toArray(int[][]::new);
    return Projection.of(indexes);
  }
}
