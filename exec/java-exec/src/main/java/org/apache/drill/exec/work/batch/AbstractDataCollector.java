/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.work.batch;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import com.google.common.collect.Maps;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.Receiver;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.record.RawFragmentBatch;

import com.google.common.base.Preconditions;

public abstract class AbstractDataCollector implements DataCollector{

  private final Map<Integer, DrillbitEndpoint> incoming;
  private final int oppositeMajorFragmentId;
  private final Map<Integer, AtomicInteger> remainders;
  private final AtomicInteger remainingRequired;
  protected final Map<Integer, RawBatchBuffer> buffers;
  private final AtomicInteger parentAccounter;
  private final AtomicInteger finishedStreams = new AtomicInteger();
  private final FragmentContext context;

  public AbstractDataCollector(AtomicInteger parentAccounter, Receiver receiver, Set<Integer> fragmentIds,
      FragmentContext context) {
    Preconditions.checkArgument(fragmentIds.size() > 0);
    Preconditions.checkNotNull(receiver);
    Preconditions.checkNotNull(parentAccounter);

    this.parentAccounter = parentAccounter;
    this.incoming = receiver.getProvidingEndpoints();
    this.remainders = Maps.newHashMap();
    for(Integer id : incoming.keySet()) {
      remainders.put(id, new AtomicInteger());
    }
    this.oppositeMajorFragmentId = receiver.getOppositeMajorFragmentId();
    this.buffers = Maps.newHashMap();
    this.context = context;
    try {
      String bufferClassName = context.getConfig().getString(ExecConstants.INCOMING_BUFFER_IMPL);
      Constructor<?> bufferConstructor = Class.forName(bufferClassName).getConstructor(FragmentContext.class, int.class);
      for(Integer fragmentId : fragmentIds) {
        buffers.put(fragmentId,
            (RawBatchBuffer) bufferConstructor.newInstance(
            context, receiver.supportsOutOfOrderExchange() ? incoming.size() : 1));
      }
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
            NoSuchMethodException | ClassNotFoundException e) {
      context.fail(e);
    }
    if (receiver.supportsOutOfOrderExchange()) {
      this.remainingRequired = new AtomicInteger(1);
    } else {
      this.remainingRequired = new AtomicInteger(fragmentIds.size());
    }
  }

  public int getOppositeMajorFragmentId() {
    return oppositeMajorFragmentId;
  }

  public Map<Integer, RawBatchBuffer> getBuffers(){
    return buffers;
  }


  public boolean batchArrived(int minorFragmentId, RawFragmentBatch batch)  throws IOException {

    // if we received an out of memory, add an item to all the buffer queues.
    if (batch.getHeader().getIsOutOfMemory()) {
      for (RawBatchBuffer buffer : buffers.values()) {
        buffer.enqueue(batch);
      }
    }

    // check to see if we have enough fragments reporting to proceed.
    boolean decremented = false;
    if (remainders.get(minorFragmentId).compareAndSet(0, 1)) {
      int rem = remainingRequired.decrementAndGet();
      if (rem == 0) {
        parentAccounter.decrementAndGet();
        decremented = true;
      }
    }

    getBuffer(minorFragmentId).enqueue(batch);

    return decremented;
  }


  @Override
  public int getTotalIncomingFragments() {
    return incoming.size();
  }

  protected abstract RawBatchBuffer getBuffer(int minorFragmentId);

  @Override
  public void close() {
  }

}