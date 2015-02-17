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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.MetricDef;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.physical.MinorFragmentEndpoint;
import org.apache.drill.exec.physical.config.HashPartitionSender;
import org.apache.drill.exec.physical.impl.BaseRootExec;
import org.apache.drill.exec.physical.impl.SendingAccountor;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.FragmentWritableBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.data.DataTunnel;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.vector.CopyUtil;

import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JType;

public class PartitionSenderRootExec extends BaseRootExec {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PartitionSenderRootExec.class);
  private RecordBatch incoming;
  private HashPartitionSender operator;
  private PartitionerDecorator partitioner;
  private FragmentContext context;
  private boolean ok = true;
  private final SendingAccountor sendCount = new SendingAccountor();
  private final int outGoingBatchCount;
  private final HashPartitionSender popConfig;
  private final StatusHandler statusHandler;
  private final double cost;

  private final AtomicIntegerArray remainingReceivers;
  private final AtomicInteger remaingReceiverCount;
  private volatile boolean done = false;
  private boolean first = true;

  long minReceiverRecordCount = Long.MAX_VALUE;
  long maxReceiverRecordCount = Long.MIN_VALUE;
  private final int numberPartitions;

  public enum Metric implements MetricDef {
    BATCHES_SENT,
    RECORDS_SENT,
    MIN_RECORDS,
    MAX_RECORDS,
    N_RECEIVERS,
    BYTES_SENT;

    @Override
    public int metricId() {
      return ordinal();
    }
  }

  public PartitionSenderRootExec(FragmentContext context,
                                 RecordBatch incoming,
                                 HashPartitionSender operator) throws OutOfMemoryException {
    super(context, new OperatorContext(operator, context, null, false), operator);
    //super(context, operator);
    this.incoming = incoming;
    this.operator = operator;
    this.context = context;
    this.outGoingBatchCount = operator.getDestinations().size();
    this.popConfig = operator;
    this.statusHandler = new StatusHandler(sendCount, context);
    this.remainingReceivers = new AtomicIntegerArray(outGoingBatchCount);
    this.remaingReceiverCount = new AtomicInteger(outGoingBatchCount);
    // Algorithm to figure out number of threads to parallelize output
    // numberOfRows/sliceTarget/numReceivers/threadfactor
    // threadFactor = 4 by default
    // one more param to put a limit on number max number of threads: default 32
    this.cost = operator.getChild().getCost();
    final OptionManager optMgr = context.getOptions();
    long sliceTarget = optMgr.getOption(ExecConstants.SLICE_TARGET).num_val;
    int threadFactor = optMgr.getOption(PlannerSettings.PARTITION_SENDER_THREADS_FACTOR.getOptionName()).num_val.intValue();
    int tmpParts = 1;
    if ( sliceTarget != 0 && outGoingBatchCount != 0 ) {
      tmpParts = (int) Math.round((((cost / (sliceTarget*1.0)) / (outGoingBatchCount*1.0)) / (threadFactor*1.0)));
      if ( tmpParts < 1) {
        tmpParts = 1;
      }
    }
    this.numberPartitions = Math.min(tmpParts, optMgr.getOption(PlannerSettings.PARTITION_SENDER_MAX_THREADS.getOptionName()).num_val.intValue());
    logger.info("Number of sending threads is: " + numberPartitions);
  }

  private boolean done() {
    for (int i = 0; i < remainingReceivers.length(); i++) {
      if (remainingReceivers.get(i) == 0) {
        return false;
      }
    }
    return true;
  }

  private void buildSchema() throws SchemaChangeException {
    createPartitioner();
    try {
      partitioner.flushOutgoingBatches(false, true);
    } catch (IOException e) {
      throw new SchemaChangeException(e);
    }
  }

  @Override
  public boolean innerNext() {

    if (!ok) {
      stop();

      return false;
    }

    IterOutcome out;
    if (!done) {
      out = next(incoming);
    } else {
      incoming.kill(true);
      out = IterOutcome.NONE;
    }

    logger.debug("Partitioner.next(): got next record batch with status {}", out);
    if (first && out == IterOutcome.OK) {
      out = IterOutcome.OK_NEW_SCHEMA;
    }
    switch(out){
      case NONE:
        try {
          // send any pending batches
          if(partitioner != null) {
            partitioner.flushOutgoingBatches(true, false);
          } else {
            sendEmptyBatch();
          }
        } catch (IOException e) {
          incoming.kill(false);
          logger.error("Error while creating partitioning sender or flushing outgoing batches", e);
          context.fail(e);
        }
        return false;

      case STOP:
        if (partitioner != null) {
          partitioner.clear();
        }
        return false;

      case OK_NEW_SCHEMA:
        try {
          // send all existing batches
          if (partitioner != null) {
            partitioner.flushOutgoingBatches(false, true);
            partitioner.clear();
          }
          createPartitioner();
          // flush to send schema downstream
          if (first) {
            first = false;
            partitioner.flushOutgoingBatches(false, true);
          }
        } catch (IOException e) {
          incoming.kill(false);
          logger.error("Error while flushing outgoing batches", e);
          context.fail(e);
          return false;
        } catch (SchemaChangeException e) {
          incoming.kill(false);
          logger.error("Error while setting up partitioner", e);
          context.fail(e);
          return false;
        }
      case OK:
        try {
          partitioner.partitionBatch(incoming);
        } catch (IOException e) {
          context.fail(e);
          incoming.kill(false);
          return false;
        }
        for (VectorWrapper<?> v : incoming) {
          v.clear();
        }
        return true;
      case NOT_YET:
      default:
        throw new IllegalStateException();
    }
  }

  private void createPartitioner() throws SchemaChangeException {

    int actualPartitions = outGoingBatchCount > numberPartitions ? numberPartitions : 1;
    int divisor = (outGoingBatchCount/numberPartitions == 0 ) ? 1 : outGoingBatchCount/numberPartitions;
    List<Partitioner> subPartitioners = createClassInstances(actualPartitions);
    for (int i = 0; i < actualPartitions; i++) {
      // TODO how to distribute remainder better especially when it is high thread count
      int startIndex = i*divisor;
      int endIndex = (i < actualPartitions - 1 ) ? (i+1)*divisor : outGoingBatchCount;
      OperatorStats partitionStats = new OperatorStats(stats, true);
      subPartitioners.get(i).setup(context, incoming, popConfig, partitionStats, sendCount, oContext, statusHandler,
        startIndex, endIndex);
    }
    partitioner = new PartitionerDecorator(subPartitioners, stats, context);
  }

  private List<Partitioner> createClassInstances(int actualPartitions) throws SchemaChangeException {
    // set up partitioning function
    final LogicalExpression expr = operator.getExpr();
    final ErrorCollector collector = new ErrorCollectorImpl();
    final ClassGenerator<Partitioner> cg ;

    cg = CodeGenerator.getRoot(Partitioner.TEMPLATE_DEFINITION, context.getFunctionRegistry());
    ClassGenerator<Partitioner> cgInner = cg.getInnerGenerator("OutgoingRecordBatch");

    final LogicalExpression materializedExpr = ExpressionTreeMaterializer.materialize(expr, incoming, collector, context.getFunctionRegistry());
    if (collector.hasErrors()) {
      throw new SchemaChangeException(String.format(
          "Failure while trying to materialize incoming schema.  Errors:\n %s.",
          collector.toErrorString()));
    }

    // generate code to copy from an incoming value vector to the destination partition's outgoing value vector
    JExpression bucket = JExpr.direct("bucket");

    // generate evaluate expression to determine the hash
    ClassGenerator.HoldingContainer exprHolder = cg.addExpr(materializedExpr);
    cg.getEvalBlock().decl(JType.parse(cg.getModel(), "int"), "bucket", exprHolder.getValue().mod(JExpr.lit(outGoingBatchCount)));
    cg.getEvalBlock()._return(cg.getModel().ref(Math.class).staticInvoke("abs").arg(bucket));

    CopyUtil.generateCopies(cgInner, incoming, incoming.getSchema().getSelectionVectorMode() == SelectionVectorMode.FOUR_BYTE);

    try {
      // compile and setup generated code
      List<Partitioner> subPartitioners = context.getImplementationClass(cg, actualPartitions);
      return subPartitioners;

    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException("Failure while attempting to load generated class", e);
    }
  }


  public void updateStats(List<? extends PartitionOutgoingBatch> outgoing) {
    long records = 0;
    for (PartitionOutgoingBatch o : outgoing) {
      long totalRecords = o.getTotalRecords();
      minReceiverRecordCount = Math.min(minReceiverRecordCount, totalRecords);
      maxReceiverRecordCount = Math.max(maxReceiverRecordCount, totalRecords);
      records += totalRecords;
    }
    stats.addLongStat(Metric.BATCHES_SENT, 1);
    stats.addLongStat(Metric.RECORDS_SENT, records);
    stats.setLongStat(Metric.MIN_RECORDS, minReceiverRecordCount);
    stats.setLongStat(Metric.MAX_RECORDS, maxReceiverRecordCount);
    stats.setLongStat(Metric.N_RECEIVERS, outgoing.size());
  }

  @Override
  public void receivingFragmentFinished(FragmentHandle handle) {
    final int id = handle.getMinorFragmentId();
    if (remainingReceivers.compareAndSet(id, 0, 1)) {
      partitioner.getOutgoingBatches(id).terminate();
      int remaining = remaingReceiverCount.decrementAndGet();
      if (remaining == 0) {
        done = true;
      }
    }
  }

  public void stop() {
    logger.debug("Partition sender stopping.");
    ok = false;
    if (partitioner != null) {
      partitioner.clear();
    }
    sendCount.waitForSendComplete();

    if (!statusHandler.isOk()) {
      context.fail(statusHandler.getException());
    }

    oContext.close();
    incoming.cleanup();
  }

  public void sendEmptyBatch() {
    FragmentHandle handle = context.getHandle();
    StatusHandler statusHandler = new StatusHandler(sendCount, context);
    for (MinorFragmentEndpoint destination : popConfig.getDestinations()) {
      FragmentHandle opposite = context.getHandle().toBuilder()
          .setMajorFragmentId(popConfig.getOppositeMajorFragmentId())
          .setMinorFragmentId(destination.getId())
          .build();
      DataTunnel tunnel = context.getDataTunnel(destination.getEndpoint(), opposite);
      FragmentWritableBatch writableBatch = FragmentWritableBatch.getEmptyLastWithSchema(
              handle.getQueryId(),
              handle.getMajorFragmentId(),
              handle.getMinorFragmentId(),
              operator.getOppositeMajorFragmentId(),
              destination.getId(),
              incoming.getSchema());
      stats.startWait();
      try {
        tunnel.sendRecordBatch(statusHandler, writableBatch);
      } finally {
        stats.stopWait();
      }
      this.sendCount.increment();
    }
  }

}
