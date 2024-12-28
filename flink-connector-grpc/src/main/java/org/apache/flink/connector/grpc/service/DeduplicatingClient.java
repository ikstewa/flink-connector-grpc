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
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.table.data.RowData;
import org.checkerframework.checker.index.qual.NonNegative;

class DeduplicatingClient implements GrpcServiceClient {

  // TODO: Can this be loaded from `table.exec.async-lookup.timeout` or GRPC Deadlines??
  private static final Duration EXPIRE_MAX = Duration.ofMinutes(10);
  private static final Duration EXPIRE_AFTER_COMPLETE = Duration.ofSeconds(5);
  private static final long MAX_CACHE_SIZE = 500;

  private final GrpcServiceClient delegate;

  private final Cache<RowData, CompletableFuture<RowData>> futureCache;

  public DeduplicatingClient(GrpcServiceClient client) {
    this.delegate = client;
    this.futureCache =
        Caffeine.newBuilder()
            .maximumSize(MAX_CACHE_SIZE)
            .scheduler(Scheduler.systemScheduler())
            .expireAfter(new BoundedExpiry<RowData, CompletableFuture<RowData>>(EXPIRE_MAX))
            .weakValues()
            .build();
  }

  @Override
  public void close() throws IOException {
    this.delegate.close();
  }

  @Override
  public CompletableFuture<RowData> asyncCall(RowData req) {
    return this.futureCache.get(
        req,
        key ->
            this.delegate
                .asyncCall(key)
                .whenComplete( // Set the expiration after future is completed
                    (val, err) ->
                        this.futureCache
                            .policy()
                            .expireVariably()
                            .get()
                            .setExpiresAfter(key, EXPIRE_AFTER_COMPLETE)));
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
    public long expireAfterUpdate(
        K key, V value, long currentTime, @NonNegative long currentDuration) {
      return currentDuration;
    }

    @Override
    public long expireAfterRead(
        K key, V value, long currentTime, @NonNegative long currentDuration) {
      return currentDuration;
    }
  }
}
