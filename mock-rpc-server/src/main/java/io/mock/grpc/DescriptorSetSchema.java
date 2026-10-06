//
// Copyright 2026 Ian Stewart
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

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.TypeRegistry;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Linked view of a {@link FileDescriptorSet}: its services and a registry of its types. */
final class DescriptorSetSchema {

  private final Map<String, ServiceDescriptor> services;
  private final TypeRegistry typeRegistry;

  private DescriptorSetSchema(Map<String, ServiceDescriptor> services, TypeRegistry typeRegistry) {
    this.services = services;
    this.typeRegistry = typeRegistry;
  }

  /**
   * Links every file in the set in dependency order.
   *
   * @throws IllegalArgumentException if an import is missing from the set or a file is invalid
   */
  static DescriptorSetSchema link(FileDescriptorSet set) {
    final Map<String, FileDescriptorProto> protos = new HashMap<>();
    set.getFileList().forEach(f -> protos.put(f.getName(), f));

    final Map<String, FileDescriptor> linked = new LinkedHashMap<>();
    for (var proto : set.getFileList()) {
      linkFile(proto, protos, linked, new HashSet<>());
    }

    final Map<String, ServiceDescriptor> services = new HashMap<>();
    final var registry = TypeRegistry.newBuilder();
    for (var file : linked.values()) {
      file.getServices().forEach(s -> services.put(s.getFullName(), s));
      registry.add(file.getMessageTypes());
    }
    return new DescriptorSetSchema(services, registry.build());
  }

  TypeRegistry typeRegistry() {
    return typeRegistry;
  }

  /**
   * Finds a unary method by its full name, {@code package.Service/Method}.
   *
   * @throws IllegalArgumentException if the service or method is unknown or the method streams
   */
  MethodDescriptor findUnaryMethod(String fullMethodName) {
    final int slash = fullMethodName.lastIndexOf('/');
    if (slash < 0) {
      throw new IllegalArgumentException(
          String.format(
              "Expected method name 'package.Service/Method' but got '%s'", fullMethodName));
    }
    final var serviceName = fullMethodName.substring(0, slash);
    final var methodName = fullMethodName.substring(slash + 1);

    final var service = services.get(serviceName);
    if (service == null) {
      throw new IllegalArgumentException(
          String.format(
              "Unknown service '%s' in descriptor set. Known services: %s",
              serviceName, services.keySet()));
    }
    final var method = service.findMethodByName(methodName);
    if (method == null) {
      throw new IllegalArgumentException(
          String.format("Unknown method '%s' on service '%s'", methodName, serviceName));
    }
    if (method.isClientStreaming() || method.isServerStreaming()) {
      throw new IllegalArgumentException(
          String.format(
              "Method '%s' is streaming. Only unary methods are supported", fullMethodName));
    }
    return method;
  }

  private static FileDescriptor linkFile(
      FileDescriptorProto proto,
      Map<String, FileDescriptorProto> protos,
      Map<String, FileDescriptor> linked,
      Set<String> inProgress) {
    final var name = proto.getName();
    final var done = linked.get(name);
    if (done != null) {
      return done;
    }
    if (!inProgress.add(name)) {
      throw new IllegalArgumentException(String.format("Circular import involving '%s'", name));
    }

    final var deps = new FileDescriptor[proto.getDependencyCount()];
    for (int i = 0; i < deps.length; i++) {
      final var depName = proto.getDependency(i);
      final var depProto = protos.get(depName);
      if (depProto == null) {
        throw new IllegalArgumentException(
            String.format(
                "'%s' imports '%s', which is missing from the descriptor set", name, depName));
      }
      deps[i] = linkFile(depProto, protos, linked, inProgress);
    }

    try {
      final var file = FileDescriptor.buildFrom(proto, deps, false);
      linked.put(name, file);
      return file;
    } catch (DescriptorValidationException e) {
      throw new IllegalArgumentException(String.format("Invalid descriptor for '%s'", name), e);
    }
  }
}
