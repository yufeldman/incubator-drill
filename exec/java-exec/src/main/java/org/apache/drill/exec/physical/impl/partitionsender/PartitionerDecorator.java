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
package org.apache.drill.exec.physical.impl.partitionsender;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.rpc.NamedThreadFactory;

import com.google.common.collect.Lists;

public class PartitionerDecorator {

  private   static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PartitionerDecorator.class);

  private static ExecutorService EXECUTOR = Executors.newCachedThreadPool(new NamedThreadFactory("Partitioner-"));

  private Partitioner [] partitioners;
  private final OperatorStats stats;

  public PartitionerDecorator(Partitioner [] partitioners, OperatorStats stats) {
    this.partitioners = partitioners;
    this.stats = stats;
  }

  public void partitionBatch(final RecordBatch incoming) throws IOException {
    // some optimization
    if (partitioners.length == 1 ) {
      // no need for threads
      partitioners[0].getStats().clear();
      logger.debug("incoming size: {}", incoming.getRecordCount());
      partitioners[0].partitionBatch(incoming);
      stats.mergeStats(partitioners[0].getStats());
      return;
    }

    List<Future<Object>> futures = Lists.newArrayList();
    for (final Partitioner part : partitioners ) {
      part.getStats().clear();
      futures.add(EXECUTOR.submit(new Callable<Object>() {

        @Override
        public Object call() throws IOException {
          logger.debug("incoming size: {}", incoming.getRecordCount());
            part.partitionBatch(incoming);
            stats.mergeStats(part.getStats());
            return null;
        }
       }));
    }
    for ( Future<Object> future : futures) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new IOException(e);
      }
    }
  }
  public void flushOutgoingBatches(boolean isLastBatch, boolean schemaChanged) throws IOException {
    for (Partitioner part : partitioners ) {
      part.getStats().clear();
      part.flushOutgoingBatches(isLastBatch, schemaChanged);
      stats.mergeStats(part.getStats());
    }
  }
  public void initialize() {
    for (Partitioner part : partitioners ) {
      part.initialize();
    }
  }

  public void clear() {
    for (Partitioner part : partitioners ) {
      part.clear();
    }
  }
  public PartitionOutgoingBatch getOutgoingBatches(int index) {
    for (Partitioner part : partitioners ) {
      PartitionOutgoingBatch outBatch = part.getOutgoingBatch(index);
      if ( outBatch != null ) {
        return outBatch;
      }
    }
    return null;
  }
}
