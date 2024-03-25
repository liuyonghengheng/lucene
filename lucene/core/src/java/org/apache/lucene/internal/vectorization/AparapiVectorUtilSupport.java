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

import com.aparapi.Kernel;
import com.aparapi.Range;

final class AparapiVectorUtilSupport implements VectorUtilSupport {

  @Override
  public float dotProduct(float[] a, float[] b) {
    final float[] mul = new float[a.length];
    final float[] v1 = a;
    final float[] v2 = b;
    Kernel kernel = new Kernel() {
      @Override
      public void run() {
        int gid = getGlobalId(0);
        mul[gid] = v1[gid] * v2[gid];
      }
    };
    kernel.execute(Range.create(v1.length));
    float sum = 0;
    for (int i = 0; i < v1.length; i++) {
      sum += mul[i];
    }
    kernel.dispose();
    return sum;
  }

  @Override
  public float cosine(float[] a, float[] b) {
    final float[] v1 = a;
    final float[] v2 = b;
    final float[] mul = new float[v1.length];
    final float[] square1 = new float[v1.length];
    final float[] square2 = new float[v1.length];
    Kernel kernel = new Kernel() {
      @Override
      public void run() {
        int gid = getGlobalId();
        mul[gid] = v1[gid] * v2[gid];
        square1[gid] = v1[gid] * v1[gid];
        square2[gid] = v2[gid] * v2[gid];
      }
    };
    kernel.execute(Range.create(v1.length));
    float sum = 0.0f;
    float sum1 = 0.0f;
    float sum2 = 0.0f;

    for (int i = 0; i < v1.length; i++) {
      sum += mul[i];
      sum1 += square1[i];
      sum2 += square2[i];
    }
    kernel.dispose();
    return sum / (float) Math.sqrt((double) sum1 * (double) sum2);
  }

  @Override
  public float squareDistance(float[] a, float[] b) {
    final float[] v1 = a;
    final float[] v2 = b;
    final float[] square = new float[v1.length];
    Kernel kernel = new Kernel() {
      @Override
      public void run() {
        int gid = getGlobalId(0);
        square[gid] = (v1[gid] - v2[gid])*(v1[gid] - v2[gid]);
      }
    };
    kernel.execute(Range.create(v1.length));
    float sum = 0;
    for (int i = 0; i < v1.length; i++) {
      sum += square[i];
    }
    kernel.dispose();
    return sum;
  }

  @Override
  public int dotProduct(byte[] a, byte[] b) {
    final int[] mul = new int[a.length];
    final byte[] v1 = a;
    final byte[] v2 = b;
    Kernel kernel = new Kernel() {
      @Override
      public void run() {
        int gid = getGlobalId(0);
        mul[gid] = v1[gid] * v2[gid];
      }
    };
    kernel.execute(Range.create(v1.length));
    int sum = 0;
    for (int i = 0; i < v1.length; i++) {
      sum += mul[i];
    }
    kernel.dispose();
    return sum;
  }

  @Override
  public float cosine(byte[] a, byte[] b) {
    final byte[] v1 = a;
    final byte[] v2 = b;
    final int[] mul = new int[v1.length];
    final int[] square1 = new int[v1.length];
    final int[] square2 = new int[v1.length];
    Kernel kernel = new Kernel() {
      @Override
      public void run() {
        int gid = getGlobalId();
        mul[gid] = v1[gid] * v2[gid];
        square1[gid] = v1[gid] * v1[gid];
        square2[gid] = v2[gid] * v2[gid];
      }
    };
    kernel.execute(Range.create(v1.length));
    int sum = 0;
    int sum1 = 0;
    int sum2 = 0;

    for (int i = 0; i < v1.length; i++) {
      sum += mul[i];
      sum1 += square1[i];
      sum2 += square2[i];
    }
    kernel.dispose();
    return (float) sum / (float) Math.sqrt((double)sum1 * (double)sum2);
  }

  @Override
  public int squareDistance(byte[] a, byte[] b) {
    final byte[] v1 = a;
    final byte[] v2 = b;
    final int[] square = new int[v1.length];
    Kernel kernel = new Kernel() {
      @Override
      public void run() {
        int gid = getGlobalId(0);
        square[gid] = (v1[gid] - v2[gid])*(v1[gid] - v2[gid]);
      }
    };
    kernel.execute(Range.create(v1.length));
    int sum = 0;
    for (int i = 0; i < v1.length; i++) {
      sum += square[i];
    }
    kernel.dispose();
    return sum;
  }

}
