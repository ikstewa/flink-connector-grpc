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

import com.google.common.truth.Truth;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SharedResourceHolderTest {

  @Test
  @DisplayName("Handles single reference")
  void testSingle() throws IOException {
    var sharedRef =
        new SharedResourceHolder<String, MyCloseable>(MyCloseableImpl::new, MyCloseable.class);

    var ref = sharedRef.createSharedResource("my instance");

    ref.close();
    Truth.assertThat(ref.closeCount()).isEqualTo(1);
  }

  @Test
  @DisplayName("Close error is propagated")
  void testCloseError() throws IOException {
    var sharedRef =
        new SharedResourceHolder<String, MyCloseable>(MyCloseableImpl::new, MyCloseable.class);

    var ref = sharedRef.createSharedResource("my instance");

    ref.close();

    final var err = Assertions.assertThrowsExactly(IOException.class, ref::close);
    Truth.assertThat(err.getMessage()).isEqualTo("CLOSED TOO MANY TIMES");
  }

  @Test
  @DisplayName("Handle multiple references")
  void testMultiRef() throws IOException {
    var sharedRef =
        new SharedResourceHolder<String, MyCloseable>(MyCloseableImpl::new, MyCloseable.class);

    var ref1 = sharedRef.createSharedResource("my instance");
    var ref2 = sharedRef.createSharedResource("my instance");
    var refOther = sharedRef.createSharedResource("my other instance");

    ref1.close();
    ref2.close();
    refOther.close();

    Truth.assertThat(ref1.closeCount()).isEqualTo(1);
    Truth.assertThat(ref2.closeCount()).isEqualTo(1);
    Truth.assertThat(refOther.closeCount()).isEqualTo(1);
  }

  private interface MyCloseable extends Closeable {
    int closeCount();

    String name();
  }

  private class MyCloseableImpl implements MyCloseable {

    final AtomicInteger closeCount = new AtomicInteger();
    final String name;

    public MyCloseableImpl(String name) {
      this.name = name;
    }

    @Override
    public void close() throws IOException {
      if (this.closeCount.getAndIncrement() != 0) {
        throw new IOException("CLOSED TOO MANY TIMES");
      }
    }

    @Override
    public int closeCount() {
      return closeCount.get();
    }

    @Override
    public String name() {
      return name;
    }
  }
}
