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
import java.util.function.BiFunction;
import org.apache.flink.table.data.RowData;

/** Marker interface to ensure the RowData combiner method is serializable. */
interface ResultRowBuilder extends BiFunction<RowData, RowData, RowData>, Serializable {}
;
