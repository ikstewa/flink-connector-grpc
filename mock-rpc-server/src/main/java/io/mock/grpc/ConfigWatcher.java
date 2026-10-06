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
package io.mock.grpc;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class ConfigWatcher {
  private static final Logger LOG = LogManager.getLogger(ConfigWatcher.class);

  private final MockJsonRpcServer server;
  private final Path path;
  private volatile WatchService watchService;

  public ConfigWatcher(MockJsonRpcServer server, Path path) {
    this.server = server;
    this.path = path;
  }

  public void start() throws IOException, InterruptedException {
    if (!path.toFile().isDirectory()) {
      LOG.info("Direct config file provided. Not watching for changes.");
      this.startServer();
    } else {
      LOG.info("Watching for config changes in '{}'", path);

      this.watchService = FileSystems.getDefault().newWatchService();
      path.register(
          this.watchService,
          StandardWatchEventKinds.ENTRY_CREATE,
          StandardWatchEventKinds.ENTRY_DELETE,
          StandardWatchEventKinds.ENTRY_MODIFY);

      new Thread(
              () -> {
                try {
                  try {
                    this.startServer();
                  } catch (Exception e) {
                    LOG.warn(
                        "Failed to start service with provided config. Waiting for updates...", e);
                  }
                  WatchKey key;
                  while ((key = watchService.take()) != null) {
                    // clear out events and search for new config
                    key.pollEvents();
                    try {
                      this.startServer();
                    } catch (Exception e) {
                      LOG.warn(
                          "Failed to start service with provided config. Waiting for updates...",
                          e);
                    }
                    if (!key.reset()) {
                      LOG.error("Failed to reset file watch key. Retrying...");
                    }
                  }
                } catch (Exception e) {
                  LOG.error("Unexpected exception encountered. Shutting down ConfigWatcher", e);
                  this.shutdown();
                }
              },
              "config-watcher")
          .start();
    }
  }

  public void shutdown() {
    try {
      if (this.watchService != null) {
        this.watchService.close();
      }
      if (this.server != null) {
        this.server.shutdown();
      }
    } catch (IOException e) {
      LOG.error("Caught exception closing WatchService", e);
    }
  }

  private void startServer() throws IOException {
    final var files = findConfigFiles(path);
    this.server.start(Objects.toString(files.pkl(), null), Objects.toString(files.desc(), null));
  }

  record ConfigFiles(@Nullable Path pkl, @Nullable Path desc) {}

  /**
   * Finds the {@code .pkl} config and the {@code .desc} descriptor set, ignoring every other file.
   * For a direct config file the descriptor set is the one in its parent directory.
   */
  static ConfigFiles findConfigFiles(Path path) throws IOException {
    final boolean direct = !Files.isDirectory(path);
    final var dir = direct ? path.toAbsolutePath().getParent() : path;

    final List<Path> pkls = new ArrayList<>();
    final List<Path> descs = new ArrayList<>();
    try (var paths = Files.list(dir)) {
      for (var file : paths.filter(Files::isRegularFile).toList()) {
        final var name = file.getFileName().toString();
        if (name.endsWith(".pkl")) {
          pkls.add(file);
        } else if (name.endsWith(".desc")) {
          descs.add(file);
        }
      }
    }

    if (direct) {
      pkls.clear();
      pkls.add(path);
    }
    if (pkls.size() > 1 || descs.size() > 1) {
      throw new IOException(
          String.format(
              "Expected '%s' to contain one .pkl and one .desc file. Found: %s %s",
              dir, pkls, descs));
    }
    if (pkls.isEmpty()) {
      LOG.info("No config file found in dir '{}'. Waiting for config...", dir);
      return new ConfigFiles(null, null);
    }
    if (descs.isEmpty()) {
      throw new IOException(
          String.format("Config file '%s' has no .desc descriptor set beside it", pkls.get(0)));
    }
    LOG.info("Found config file '{}' and descriptor set '{}'", pkls.get(0), descs.get(0));
    return new ConfigFiles(pkls.get(0), descs.get(0));
  }
}
