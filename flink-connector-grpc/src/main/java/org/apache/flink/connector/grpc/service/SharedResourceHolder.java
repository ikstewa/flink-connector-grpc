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

import com.google.common.reflect.Reflection;
import java.io.Closeable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Stream;

class SharedResourceHolder<K, V extends Closeable> {

  private static Method CLOSE_METHOD =
      Stream.of(Closeable.class.getMethods())
          .filter(m -> m.getName().equals("close"))
          .findFirst()
          .get();

  private final ConcurrentHashMap<K, SharedRef<V>> storage;
  private final Function<K, V> factory;
  private final Class<V> resourceInterface;

  public SharedResourceHolder(Function<K, V> resourceFactory, Class<V> resourceInterface) {
    this.storage = new ConcurrentHashMap<>();
    this.factory = Objects.requireNonNull(resourceFactory);
    this.resourceInterface = Objects.requireNonNull(resourceInterface);
  }

  public V createSharedResource(K key) {
    final var sharedRef =
        storage.compute(
            key,
            (k, v) -> {
              if (v == null) {
                return new SharedRef<>(1, proxyClosable(k, this.factory.apply(k)));
              } else {
                return new SharedRef<>(v.count() + 1, v.resource());
              }
            });
    return sharedRef.resource();
  }

  /**
   * Wrap the provided object in a proxy which on invocation of the close method, decrements the ref
   * counter. Once all references have closed, the original close is invoked.
   */
  private V proxyClosable(K key, V original) {
    return Reflection.newProxy(
        resourceInterface,
        (proxy, method, args) -> {
          if (method.equals(CLOSE_METHOD)) {
            final var clientRef =
                this.storage.compute(
                    key,
                    (k, v) -> {
                      if (v == null) {
                        return null; // Should never happen unless bug
                      } else if (v.count() == 1) {
                        return null;
                      } else {
                        return new SharedRef<>(v.count() - 1, v.resource());
                      }
                    });
            if (clientRef != null) {
              // Skip original method call, more references left
              return null;
            }
          }
          try {
            return method.invoke(original, args);
          } catch (InvocationTargetException e) {
            throw e.getCause();
          }
        });
  }

  private record SharedRef<V>(int count, V resource) {}
}
