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
      this.server.start(findConfigFile(path));
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
                    this.server.start(findConfigFile(path));
                  } catch (Exception e) {
                    LOG.warn(
                        "Failed to start service with provided config. Waiting for updates...", e);
                  }
                  WatchKey key;
                  while ((key = watchService.take()) != null) {
                    // clear out events and search for new config
                    key.pollEvents();
                    try {
                      this.server.start(findConfigFile(path));
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

  private static String findConfigFile(Path path) throws IOException {
    try (var paths = Files.walk(path)) {
      final var files = paths.filter(Files::isRegularFile).toList();
      if (files.size() == 1) {
        final var configFile = files.iterator().next().toString();
        LOG.info("Found config file '{}'", configFile);
        return configFile;
      } else if (files.size() > 1) {
        throw new IOException(
            String.format(
                "Expected config dirctory to contain only a single file. Found: %s", files));
      } else {
        LOG.info("No config file found in dir '{}'. Waiting for config...", path);
        return null;
      }
    }
  }
}
