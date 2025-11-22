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
package org.apache.flink.connector.grpc.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;
import org.apache.flink.table.data.RowData;

class DeduplicatingClient implements GrpcServiceClient {

  // TODO: Can this be loaded from `table.exec.async-lookup.timeout` or GRPC Deadlines??
  private static final Duration EXPIRE_MAX = Duration.ofMinutes(10);
  private static final Duration EXPIRE_AFTER_COMPLETE = Duration.ofSeconds(5);
  private static final long MAX_CACHE_SIZE = 500;

  private final GrpcServiceClient delegate;

  private final Cache<RowData, ListenableFuture<RowData>> futureCache;

  public DeduplicatingClient(GrpcServiceClient client) {
    this.delegate = client;
    this.futureCache =
        Caffeine.newBuilder()
            .maximumSize(MAX_CACHE_SIZE)
            .scheduler(Scheduler.systemScheduler())
            .expireAfter(new BoundedExpiry<RowData, ListenableFuture<RowData>>(EXPIRE_MAX))
            .weakValues()
            .build();
  }

  @Override
  public void close() throws IOException {
    this.delegate.close();
  }

  @Override
  public ListenableFuture<RowData> asyncCall(RowData req, @Nullable Executor executor) {
    return this.futureCache.get(
        req,
        key -> {
          final var fut = this.delegate.asyncCall(key, executor);
          fut.addListener(
              () ->
                  this.futureCache
                      .policy()
                      .expireVariably()
                      .get()
                      .setExpiresAfter(key, EXPIRE_AFTER_COMPLETE),
              MoreExecutors.directExecutor());
          return fut;
        });
  }

  private static class BoundedExpiry<K, V> implements Expiry<K, V> {

    private final long maxExpireNanos;

    public BoundedExpiry(Duration maxExpire) {
      this.maxExpireNanos = maxExpire.toNanos();
    }

    @Override
    public long expireAfterCreate(K key, V value, long currentTime) {
      return maxExpireNanos;
    }

    @Override
    public long expireAfterUpdate(K key, V value, long currentTime, long currentDuration) {
      return currentDuration;
    }

    @Override
    public long expireAfterRead(K key, V value, long currentTime, long currentDuration) {
      return currentDuration;
    }
  }
}
