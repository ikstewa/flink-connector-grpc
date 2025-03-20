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

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.StatusRuntimeException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.grpc.handler.GrpcResponseHandler;
import org.apache.flink.connector.grpc.service.GrpcServiceClient;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AsyncGrpcLookupFunction extends AsyncLookupFunction {

  private static final Logger LOG = LogManager.getLogger(GrpcServiceClient.class);

  // TODO: Configurable options??
  private static final int REQUEST_THREAD_POOL_SIZE = 8;
  private static final int PUBLISHING_THREAD_POOL_SIZE = 4;
  private static final UncaughtExceptionHandler LOGGING_EXCEPTION_HANDLER =
      (t, e) -> LOG.error("Thread:" + t + " exited with Exception.", e);

  private final GrpcServiceOptions grpcConfig;
  private final SerializationSchema<RowData> requestSchema;
  private final DeserializationSchema<RowData> responseSchema;
  private final Function<RowData, RowData> requestHandler;
  private final GrpcResponseHandler<RowData, RowData, RowData> responseHandler;
  private final boolean useDirectExecutor;

  private transient GrpcServiceClient grpcClient;
  private transient AtomicInteger grpcCallCounter;
  private transient AtomicInteger grpcErrorCounter;
  private transient Executor requestExecutor;
  private transient Executor publishingExecutor;

  public AsyncGrpcLookupFunction(
      GrpcServiceOptions grpcConfig,
      Function<RowData, RowData> requestHandler,
      GrpcResponseHandler<RowData, RowData, RowData> responseHandler,
      SerializationSchema<RowData> requestSchema,
      DeserializationSchema<RowData> responseSchema) {
    this(grpcConfig, requestHandler, responseHandler, requestSchema, responseSchema, false);
  }

  protected AsyncGrpcLookupFunction(
      GrpcServiceOptions grpcConfig,
      Function<RowData, RowData> requestHandler,
      GrpcResponseHandler<RowData, RowData, RowData> responseHandler,
      SerializationSchema<RowData> requestSchema,
      DeserializationSchema<RowData> responseSchema,
      boolean useDirectExecutor) {
    this.grpcConfig = grpcConfig;
    this.requestSchema = requestSchema;
    this.responseSchema = responseSchema;
    this.requestHandler = requestHandler;
    this.responseHandler = responseHandler;
    this.useDirectExecutor = useDirectExecutor;
  }

  @Override
  public void open(FunctionContext context) throws Exception {
    super.open(context);
    this.requestSchema.open(null);
    this.responseSchema.open(null);

    this.grpcClient =
        GrpcServiceClient.createClient(this.grpcConfig, this.requestSchema, this.responseSchema);

    this.grpcCallCounter = new AtomicInteger(0);
    this.grpcErrorCounter = new AtomicInteger(0);
    context
        .getMetricGroup()
        .gauge("grpc-table-lookup-call-counter", () -> grpcCallCounter.intValue());
    context
        .getMetricGroup()
        .gauge("grpc-table-lookup-call-error", () -> grpcErrorCounter.intValue());

    if (useDirectExecutor) {
      this.requestExecutor = MoreExecutors.directExecutor();
      this.publishingExecutor = MoreExecutors.directExecutor();
    } else {
      this.requestExecutor =
          Executors.newFixedThreadPool(
              REQUEST_THREAD_POOL_SIZE,
              new ExecutorThreadFactory("grpc-async-lookup-worker", LOGGING_EXCEPTION_HANDLER));

      this.publishingExecutor =
          Executors.newFixedThreadPool(
              PUBLISHING_THREAD_POOL_SIZE,
              new ExecutorThreadFactory("grpc-async-publishing-worker", LOGGING_EXCEPTION_HANDLER));
    }
  }

  @Override
  public void close() throws Exception {
    this.grpcClient.close();
    if (this.requestExecutor instanceof ExecutorService execService) {
      execService.shutdownNow();
    }
    if (this.publishingExecutor instanceof ExecutorService execService) {
      execService.shutdownNow();
    }
    super.close();
  }

  @Override
  public CompletableFuture<Collection<RowData>> asyncLookup(RowData req) {
    LOG.debug("Received async lookup request for row: {}", req);
    this.grpcCallCounter.incrementAndGet();

    // Trim request: Metadata filters can show up in request row
    final RowData keyRow = this.requestHandler.apply(req);

    final var grpcCall = this.grpcClient.asyncCall(keyRow, this.requestExecutor);

    final var transformedResult =
        Futures.transform(
            grpcCall,
            r -> {
              LOG.debug("Handling response: request[{}] response[{}] error[{}]", keyRow, r, null);
              final var result = this.responseHandler.handle(keyRow, r, null);
              LOG.debug("Returning response row: {}", result);
              return List.of(result);
            },
            this.publishingExecutor);

    final var resultFut =
        Futures.catching(
            transformedResult,
            Exception.class,
            err -> {
              this.grpcErrorCounter.incrementAndGet();

              final var statusErr = findStatusError(err);

              // Propagate any non-status exceptions
              if (statusErr.isEmpty()) {
                LOG.error("Found unexpected result error", err);
                throw new CompletionException(err);
              }

              LOG.debug(
                  "Handling response: request[{}] response[{}] error[{}]", keyRow, null, statusErr);
              final var result = this.responseHandler.handle(keyRow, null, statusErr.orElse(null));
              LOG.debug("Returning response row: {}", result);
              return List.of(result);
            },
            this.publishingExecutor);

    // Transform to CompletableFuture
    final var fut = new CompletableFuture<Collection<RowData>>();
    Futures.addCallback(
        resultFut,
        new FutureCallback<>() {
          public void onFailure(Throwable throwable) {
            fut.completeExceptionally(throwable);
          }

          public void onSuccess(List<RowData> t) {
            fut.complete(t);
          }
        },
        this.publishingExecutor);
    return fut;
  }

  private static Optional<StatusRuntimeException> findStatusError(Throwable err) {
    return Stream.ofNullable(err)
        .map(Throwables::getCausalChain)
        .flatMap(List::stream)
        .filter(StatusRuntimeException.class::isInstance)
        .map(StatusRuntimeException.class::cast)
        .findFirst();
  }
}
