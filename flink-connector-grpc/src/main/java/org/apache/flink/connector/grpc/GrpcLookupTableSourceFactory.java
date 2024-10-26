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

import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

import com.google.auto.service.AutoService;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.grpc.util.MethodDescriptorUtils;
import org.apache.flink.connector.grpc.util.MethodDescriptorUtils.FlinkMethodDescriptor;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;
import org.apache.flink.table.factories.SerializationFormatFactory;

@AutoService(Factory.class)
public class GrpcLookupTableSourceFactory implements DynamicTableSourceFactory {

  @Override
  public String factoryIdentifier() {
    return "grpc-lookup";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Set.of(GrpcConnectorOptions.HOST, GrpcConnectorOptions.PORT);
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Set.of(
        GrpcConnectorOptions.PLAIN_TEXT,
        GrpcConnectorOptions.RPC_METHOD_DESC,
        GrpcConnectorOptions.RPC_METHOD,
        GrpcConnectorOptions.REQUEST_FORMAT,
        GrpcConnectorOptions.RESPONSE_FORMAT,
        LookupOptions.CACHE_TYPE,
        LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS,
        LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE,
        LookupOptions.PARTIAL_CACHE_MAX_ROWS,
        LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY,
        LookupOptions.MAX_RETRIES);
  }

  @Override
  public Set<ConfigOption<?>> forwardOptions() {
    return Set.of();
  }

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    final TableFactoryHelper helper = createTableFactoryHelper(this, context);
    final ReadableConfig tableOptions = helper.getOptions();

    validateSourceOptions(tableOptions);

    final var desc = parseDescriptorOptions(tableOptions, helper);

    // validate all options
    helper.validate();

    return new GrpcLookupTableSource(
        new GrpcServiceOptions(
            tableOptions.get(GrpcConnectorOptions.HOST),
            tableOptions.get(GrpcConnectorOptions.PORT),
            tableOptions.get(GrpcConnectorOptions.PLAIN_TEXT),
            desc.serviceMethodName(),
            tableOptions.get(LookupOptions.MAX_RETRIES)),
        context.getPhysicalRowDataType(),
        desc.requestEncoder(),
        desc.responseDecoder(),
        getLookupCache(tableOptions));
  }

  protected void validateSourceOptions(ReadableConfig tableOptions)
      throws IllegalArgumentException {
    // Ensure that one of method name or descriptor is provided
    final var methodOpt = tableOptions.getOptional(GrpcConnectorOptions.RPC_METHOD);
    final var descOpt = tableOptions.getOptional(GrpcConnectorOptions.RPC_METHOD_DESC);

    if (!(methodOpt.isEmpty() ^ descOpt.isEmpty())) {
      throw new IllegalArgumentException(
          String.format(
              "Required only one of '%s' or '%s'",
              GrpcConnectorOptions.RPC_METHOD.key(), GrpcConnectorOptions.RPC_METHOD_DESC.key()));
    }

    if (methodOpt.isPresent()) {
      // Format is required when configuring using method
      if (tableOptions.getOptional(GrpcConnectorOptions.REQUEST_FORMAT).isEmpty()
          || tableOptions.getOptional(GrpcConnectorOptions.RESPONSE_FORMAT).isEmpty()) {
        throw new IllegalArgumentException(
            String.format(
                "Config options '%s' and '%s' are required, if '%s' is configured.",
                GrpcConnectorOptions.REQUEST_FORMAT,
                GrpcConnectorOptions.RESPONSE_FORMAT,
                GrpcConnectorOptions.RPC_METHOD));
      }
    }
  }

  protected FlinkMethodDescriptor parseDescriptorOptions(
      ReadableConfig tableOptions, TableFactoryHelper helper) {
    if (tableOptions.getOptional(GrpcConnectorOptions.RPC_METHOD).isPresent()) {
      return new FlinkMethodDescriptor(
          tableOptions.get(GrpcConnectorOptions.RPC_METHOD),
          helper.discoverDecodingFormat(
              DeserializationFormatFactory.class, GrpcConnectorOptions.RESPONSE_FORMAT),
          helper.discoverEncodingFormat(
              SerializationFormatFactory.class, GrpcConnectorOptions.REQUEST_FORMAT));
    } else {
      return MethodDescriptorUtils.parseMethodDescriptor(
          tableOptions.get(GrpcConnectorOptions.RPC_METHOD_DESC));
    }
  }

  @Nullable
  private LookupCache getLookupCache(ReadableConfig tableOptions) {
    return switch (tableOptions.get(LookupOptions.CACHE_TYPE)) {
      case NONE -> null;
      case PARTIAL -> DefaultLookupCache.fromConfig(tableOptions);
      case FULL -> throw new UnsupportedOperationException("Only supports 'PARTIAL' cache");
    };
  }
}
