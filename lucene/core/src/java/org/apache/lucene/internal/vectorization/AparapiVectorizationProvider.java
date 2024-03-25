/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.internal.vectorization;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.logging.Logger;
import org.apache.lucene.util.SuppressForbidden;

/** A vectorization provider that leverages the Panama Vector API. */
final class AparapiVectorizationProvider extends VectorizationProvider {

  private final VectorUtilSupport vectorUtilSupport;

  // Extracted to a method to be able to apply the SuppressForbidden annotation
  @SuppressWarnings("removal")
  @SuppressForbidden(reason = "security manager")
  private static <T> T doPrivileged(PrivilegedAction<T> action) {
    return AccessController.doPrivileged(action);
  }

  AparapiVectorizationProvider() {

    this.vectorUtilSupport = new AparapiVectorUtilSupport();
    var log = Logger.getLogger(getClass().getName());
    log.info(
            "Opencl Java Aparapi API enabled");
  }

  @Override
  public VectorUtilSupport getVectorUtilSupport() {
    return vectorUtilSupport;
  }
}
