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

/**
 * Decorator class to hide multiple Partitioner existence from the caller
 * since this class involves multithreading processing of incoming batches
 * as well as flushing it needs special handling of OperatorStats - stats
 * since stats are not suitable for use in multithreaded environment
 * The algorithm to figure out processing versus wait time is based on following formula:
 * totalWaitTime = totalAllPartitionersProcessingTime - max(sum(processingTime) by partitioner)
 */
public class PartitionerDecorator {

  private   static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PartitionerDecorator.class);

  private static ExecutorService EXECUTOR = Executors.newCachedThreadPool(new NamedThreadFactory("Partitioner-"));

  private Partitioner [] partitioners;
  private final OperatorStats stats;
  private final String tName;


  public PartitionerDecorator(Partitioner [] partitioners, OperatorStats stats) {
    this.partitioners = partitioners;
    this.stats = stats;
    this.tName = Thread.currentThread().getName();
  }

  /**
   * partitionBatch - decorator method to call real Partitioner(s) to process incoming batch
   * uses either threading or not threading approach based on number Partitioners
   * @param incoming
   * @throws IOException
   */
  public void partitionBatch(final RecordBatch incoming) throws IOException {
    executeMethodLogic(new MyExecute (){

      @Override
      public void execute(Partitioner part) throws IOException {
        part.getStats().clear();
        logger.debug("incoming size: {}", incoming.getRecordCount());
        part.getStats().startProcessing();
        try {
          part.partitionBatch(incoming);
        } finally {
          part.getStats().stopProcessing();
          stats.mergeMetrics(part.getStats());
        }
      }});
  }

  /**
   * flushOutgoingBatches - decorator to call real Partitioner(s) flushOutgoingBatches
   * @param isLastBatch
   * @param schemaChanged
   * @throws IOException
   */
  public void flushOutgoingBatches(final boolean isLastBatch, final boolean schemaChanged) throws IOException {
    executeMethodLogic(new MyExecute (){

      @Override
      public void execute(Partitioner part) throws IOException {
        part.getStats().clear();
        part.getStats().startProcessing();
        try {
          part.flushOutgoingBatches(isLastBatch, schemaChanged);
        } finally {
          part.getStats().stopProcessing();
          stats.mergeMetrics(part.getStats());
        }
      }});
  }

  /**
   * decorator method to call multiple Partitioners initialize()
   */
  public void initialize() {
    for (Partitioner part : partitioners ) {
      part.initialize();
    }
  }

  /**
   * decorator method to call multiple Partitioners clear()
   */
  public void clear() {
    for (Partitioner part : partitioners ) {
      part.clear();
    }
  }

  /**
   * Helper method to get PartitionOutgoingBatch based on the index
   * since we may have more then one Partitioner
   * As number of Partitioners should be very small it is OK to loop
   * in order to find right partitioner
   * @param index
   * @return
   */
  public PartitionOutgoingBatch getOutgoingBatches(int index) {
    for (Partitioner part : partitioners ) {
      PartitionOutgoingBatch outBatch = part.getOutgoingBatch(index);
      if ( outBatch != null ) {
        return outBatch;
      }
    }
    return null;
  }

  /**
   * Helper to execute the different methods wrapped into same logic
   * @param iface
   * @throws IOException
   */
  private void executeMethodLogic(final MyExecute iface) throws IOException {
    if (partitioners.length == 1 ) {
      // no need for threads
      iface.execute(partitioners[0]);
      // since main stats did not have any wait time - adjust based of partitioner stats wait time
      stats.adjustWaitNanos(partitioners[0].getStats().getWaitNanos());
      return;
    }

    long maxProcessTime = 0l;
    // start waiting on main stats to adjust by sum(max(processing)) at the end
    stats.startWait();
    try {
      List<Future<Partitioner>> futures = Lists.newArrayList();
      for (final Partitioner part : partitioners ) {
        futures.add(EXECUTOR.submit(new Callable<Partitioner>() {

          @Override
          public Partitioner call() throws IOException {
            String curThreadName = "Partitioner-" + Thread.currentThread().getId() + "-" + tName;
            Thread.currentThread().setName(curThreadName);
            iface.execute(part);
            return part;
          }
         }));
      }
      for ( Future<Partitioner> future : futures) {
        try {
          Partitioner part = future.get();
          long currentProcessingNanos = part.getStats().getProcessingNanos();
          // find out max Partitioner processing time
          maxProcessTime = (currentProcessingNanos > maxProcessTime) ? currentProcessingNanos : maxProcessTime;
        } catch (InterruptedException | ExecutionException e) {
          throw new IOException(e);
        }
      }
    } finally {
      stats.stopWait();
      // scale down main stats wait time based on calculated processing time
      stats.adjustWaitNanos(-maxProcessTime);
    }

  }

  /**
   * Helper interface to generalize functionality executed in the thread
   * since it is absolutely the same for partitionBatch and flushOutgoingBatches
   *
   */
  private interface MyExecute {
    public void execute(Partitioner partitioner) throws IOException;
  }
 }
