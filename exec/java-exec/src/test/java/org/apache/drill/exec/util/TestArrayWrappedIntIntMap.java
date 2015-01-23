/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestArrayWrappedIntIntMap {

  @Test(expected=IllegalArgumentException.class)
  public void testCapacity0() {
    new ArrayWrappedIntIntMap(0);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testCapacityAboveMaxCapacity() {
    new ArrayWrappedIntIntMap(1<<16 +1);
  }

  @Test
  public void testSimple() {
    ArrayWrappedIntIntMap map = new ArrayWrappedIntIntMap(10);
    map.put(0, 0);
    map.put(1, 1);
    map.put(9, 9);

    assertEquals(map.get(0), 0);
    assertEquals(map.get(1), 1);
    assertEquals(map.get(9), 9);
  }

  @Test(expected=ArrayIndexOutOfBoundsException.class)
  public void testInvalidKeyAccess() {
    ArrayWrappedIntIntMap map = new ArrayWrappedIntIntMap(10);
    map.put(0, 0);
    map.put(1, 1);
    map.put(9, 9);

    assertEquals(map.get(0), 0);
    assertEquals(map.get(1), 1);
    assertEquals(map.get(9), 9);

    assertEquals(map.get(2), Integer.MIN_VALUE);
    map.get(35); // this should throw ArrayOutOfBoundsException
  }

  @Test
  public void testResizing() {
    ArrayWrappedIntIntMap map = new ArrayWrappedIntIntMap(10);
    int[] expectedValues = new int[] {1, 32, 64, 150};

    for(int i=0; i<expectedValues.length; i++) {
      map.put(expectedValues[i], expectedValues[i]);
    }

    for(int i=0; i<expectedValues.length; i++) {
      assertEquals(expectedValues[i], map.get(expectedValues[i]));
    }
  }
}
