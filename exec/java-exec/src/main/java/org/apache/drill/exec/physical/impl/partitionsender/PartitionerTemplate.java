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
import java.util.Iterator;
import java.util.List;

import javax.inject.Named;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.compile.sig.RuntimeOverridden;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.physical.MinorFragmentEndpoint;
import org.apache.drill.exec.physical.config.HashPartitionSender;
import org.apache.drill.exec.physical.impl.SendingAccountor;
import org.apache.drill.exec.physical.impl.partitionsender.PartitionSenderRootExec.Metric;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.FragmentWritableBatch;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.SchemaBuilder;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.rpc.data.DataTunnel;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public abstract class PartitionerTemplate implements Partitioner {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PartitionerTemplate.class);

  private SelectionVector2 sv2;
  private SelectionVector4 sv4;
  private RecordBatch incoming;
  private OperatorStats stats;
  private int start;
  private int end;
  private List<OutgoingRecordBatch> outgoingBatches = Lists.newArrayList();

  private static final String REWRITE_MSG = "Failed to write the record {} in available space. Attempting to rewrite.";
  private static final String RECORD_TOO_BIG_MSG = "Record {} is too big to fit into the allocated memory of ValueVector.";

  public PartitionerTemplate() throws SchemaChangeException {
  }

  @Override
  public List<? extends PartitionOutgoingBatch> getOutgoingBatches() {
    return outgoingBatches;
  }

  @Override
  public PartitionOutgoingBatch getOutgoingBatch(int index) {
    if ( index >= start && index < end) {
      return outgoingBatches.get(index - start);
    }
    return null;
  }

  @Override
  public final void setup(FragmentContext context,
                          RecordBatch incoming,
                          HashPartitionSender popConfig,
                          OperatorStats stats,
                          SendingAccountor sendingAccountor,
                          OperatorContext oContext,
                          StatusHandler statusHandler,
                          int start, int end) throws SchemaChangeException {

    this.incoming = incoming;
    this.stats = stats;
    this.start = start;
    this.end = end;
    doSetup(context, incoming, null);

    int fieldId = 0;
    for (MinorFragmentEndpoint destination : popConfig.getDestinations()) {
      // create outgoingBatches only for subset of Destination Points
      if ( fieldId >= start && fieldId < end ) {
        logger.debug("start: {}, count: {}, fieldId: {}", start, end, fieldId);
      FragmentHandle opposite = context.getHandle().toBuilder()
          .setMajorFragmentId(popConfig.getOppositeMajorFragmentId())
          .setMinorFragmentId(destination.getId()).build();
      outgoingBatches.add(new OutgoingRecordBatch(stats, sendingAccountor, popConfig,
          context.getDataTunnel(destination.getEndpoint(), opposite), context, oContext.getAllocator(), destination.getId(),
          statusHandler));
      }
      fieldId++;
    }

    for (OutgoingRecordBatch outgoingRecordBatch : outgoingBatches) {
      outgoingRecordBatch.initializeBatch();
    }

    SelectionVectorMode svMode = incoming.getSchema().getSelectionVectorMode();
    switch(svMode){
      case FOUR_BYTE:
        this.sv4 = incoming.getSelectionVector4();
        break;

      case TWO_BYTE:
        this.sv2 = incoming.getSelectionVector2();
        break;

      case NONE:
        break;

      default:
        throw new UnsupportedOperationException("Unknown selection vector mode: " + svMode.toString());
    }
  }

  @Override
  public OperatorStats getStats() {
    return stats;
  }

  /**
   * Flush each outgoing record batch, and optionally reset the state of each outgoing record
   * batch (on schema change).  Note that the schema is updated based on incoming at the time
   * this function is invoked.
   *
   * @param isLastBatch    true if this is the last incoming batch
   * @param schemaChanged  true if the schema has changed
   */
  @Override
  public void flushOutgoingBatches(boolean isLastBatch, boolean schemaChanged) throws IOException {
    for (OutgoingRecordBatch batch : outgoingBatches) {
      logger.debug("Attempting to flush all outgoing batches");
      if (isLastBatch) {
        batch.setIsLast();
      }
      batch.flush();
      if (schemaChanged) {
        batch.resetBatch();
        batch.initializeBatch();
      }
    }
  }

  @Override
  public void partitionBatch(RecordBatch incoming) throws IOException {
    SelectionVectorMode svMode = incoming.getSchema().getSelectionVectorMode();

    // let's assume we have hash in incoming.getSchema().column - so it will be on each record
    // for now assume 2 threads (round robin) outgoingbatch is thread agnostic
    // Keeping the for loop inside the case to avoid case evaluation for each record.
    switch(svMode) {
      case NONE:
        for (int recordId = 0; recordId < incoming.getRecordCount(); ++recordId) {
          doCopy(recordId);
        }
        break;

      case TWO_BYTE:
        for (int recordId = 0; recordId < incoming.getRecordCount(); ++recordId) {
          int svIndex = sv2.getIndex(recordId);
          doCopy(svIndex);
        }
        break;

      case FOUR_BYTE:
        for (int recordId = 0; recordId < incoming.getRecordCount(); ++recordId) {
          int svIndex = sv4.get(recordId);
          doCopy(svIndex);
        }
        break;

      default:
        throw new UnsupportedOperationException("Unknown selection vector mode: " + svMode.toString());
    }
  }

  /**
   * Helper method to copy data based on partition
   * @param svIndex
   * @param incoming
   * @throws IOException
   */
  private void doCopy(int svIndex) throws IOException {
    int index = doEval(svIndex);
    if ( index >= start && index < end) {
      OutgoingRecordBatch outgoingBatch = outgoingBatches.get(index - start);
      outgoingBatch.copy(svIndex);
    }
  }

  @Override
  public void clear() {
    for (OutgoingRecordBatch outgoingRecordBatch : outgoingBatches) {
      outgoingRecordBatch.clear();
    }
  }

  private String composeTooBigMsg(int recordId, RecordBatch incoming) {
    String msg = String.format("Record " + recordId + " is too big to fit into the allocated memory of ValueVector.");
    msg += " Schema: ";
    for (int i = 0; i < incoming.getSchema().getFieldCount(); i++) {
      MaterializedField f = incoming.getSchema().getColumn(i);
      msg += f.getPath().getRootSegment().getPath() + " ";
    }
    return msg;
  }

  public abstract void doSetup(@Named("context") FragmentContext context, @Named("incoming") RecordBatch incoming, @Named("outgoing") OutgoingRecordBatch[] outgoing) throws SchemaChangeException;
  public abstract int doEval(@Named("inIndex") int inIndex);

  public class OutgoingRecordBatch implements PartitionOutgoingBatch, VectorAccessible {

    private final DataTunnel tunnel;
    private final HashPartitionSender operator;
    private final FragmentContext context;
    private final BufferAllocator allocator;
    private final VectorContainer vectorContainer = new VectorContainer();
    private final SendingAccountor sendCount;
    private final int oppositeMinorFragmentId;

    private boolean isLast = false;
    private boolean isFirst = true;
    private volatile boolean terminated = false;
    private boolean dropAll = false;
    private BatchSchema outSchema;
    private int recordCount;
    private int totalRecords;
    private OperatorStats stats;
    private static final int DEFAULT_RECORD_BATCH_SIZE = 1000;

    private final StatusHandler statusHandler;

    public OutgoingRecordBatch(OperatorStats stats, SendingAccountor sendCount, HashPartitionSender operator, DataTunnel tunnel,
                               FragmentContext context, BufferAllocator allocator, int oppositeMinorFragmentId,
                               StatusHandler statusHandler) {
      this.context = context;
      this.allocator = allocator;
      this.operator = operator;
      this.tunnel = tunnel;
      this.sendCount = sendCount;
      this.stats = stats;
      this.oppositeMinorFragmentId = oppositeMinorFragmentId;
      this.statusHandler = statusHandler;
    }

    protected void copy(int inIndex) throws IOException {
      doEval(inIndex, recordCount);
      recordCount++;
      totalRecords++;
      if (recordCount == DEFAULT_RECORD_BATCH_SIZE) {
        flush();
      }
    }

    @Override
    public void terminate() {
      terminated = true;
    }

    @RuntimeOverridden
    protected void doSetup(@Named("incoming") RecordBatch incoming, @Named("outgoing") VectorAccessible outgoing) {};

    @RuntimeOverridden
    protected void doEval(@Named("inIndex") int inIndex, @Named("outIndex") int outIndex) { };

    public void flush() throws IOException {
      if (dropAll) {
        vectorContainer.zeroVectors();
        return;
      }
      final FragmentHandle handle = context.getHandle();

      if (recordCount != 0 && !terminated) {

        for(VectorWrapper<?> w : vectorContainer){
          w.getValueVector().getMutator().setValueCount(recordCount);
        }

        FragmentWritableBatch writableBatch = new FragmentWritableBatch(isLast,
                handle.getQueryId(),
                handle.getMajorFragmentId(),
                handle.getMinorFragmentId(),
                operator.getOppositeMajorFragmentId(),
                oppositeMinorFragmentId,
                getWritableBatch());

        updateStats(writableBatch);
        stats.startWait();
        try {
          tunnel.sendRecordBatch(statusHandler, writableBatch);
        } finally {
          stats.stopWait();
        }
        this.sendCount.increment();
      } else {
        logger.debug("Flush requested on an empty outgoing record batch" + (isLast ? " (last batch)" : ""));
        if (isFirst || isLast || terminated) {
          // send final (empty) batch
          FragmentWritableBatch writableBatch = new FragmentWritableBatch(isLast || terminated,
                  handle.getQueryId(),
                  handle.getMajorFragmentId(),
                  handle.getMinorFragmentId(),
                  operator.getOppositeMajorFragmentId(),
                  oppositeMinorFragmentId,
                  getWritableBatch());
          stats.startWait();
          try {
            tunnel.sendRecordBatch(statusHandler, writableBatch);
          } finally {
            stats.stopWait();
          }
          this.sendCount.increment();
          vectorContainer.zeroVectors();
          if (!isFirst) {
            dropAll = true;
          }
          if (isFirst) {
            isFirst = !isFirst;
          }
          return;
        }
      }

      // reset values and reallocate the buffer for each value vector based on the incoming batch.
      // NOTE: the value vector is directly referenced by generated code; therefore references
      // must remain valid.
      recordCount = 0;
      vectorContainer.zeroVectors();
      for (VectorWrapper<?> v : vectorContainer) {
        v.getValueVector().allocateNew();
      }
      if (!statusHandler.isOk()) {
        throw new IOException(statusHandler.getException());
      }
    }

    public void updateStats(FragmentWritableBatch writableBatch) {
      stats.addLongStat(Metric.BYTES_SENT, writableBatch.getByteCount());
      stats.addLongStat(Metric.BATCHES_SENT, 1);
      stats.addLongStat(Metric.RECORDS_SENT, writableBatch.getHeader().getDef().getRecordCount());
    }

    public void initializeBatch() {
      isLast = false;
      vectorContainer.clear();

      SchemaBuilder bldr = BatchSchema.newBuilder().setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE);
      for (VectorWrapper<?> v : incoming) {

        // add field to the output schema
        bldr.addField(v.getField());

        // allocate a new value vector
        ValueVector outgoingVector = TypeHelper.getNewVector(v.getField(), allocator);
        v.getValueVector().makeTransferPair(outgoingVector);
        outgoingVector.allocateNew();
        vectorContainer.add(outgoingVector);
      }
      outSchema = bldr.build();
      doSetup(incoming, vectorContainer);
    }

    public void resetBatch() {
      isLast = false;
      recordCount = 0;
      for (VectorWrapper<?> v : vectorContainer){
        v.getValueVector().clear();
      }
    }

    public void setIsLast() {
      isLast = true;
    }

    @Override
    public BatchSchema getSchema() {
      Preconditions.checkNotNull(outSchema);
      return outSchema;
    }

    @Override
    public int getRecordCount() {
      return recordCount;
    }


    @Override
    public long getTotalRecords() {
      return totalRecords;
    }

    @Override
    public TypedFieldId getValueVectorId(SchemaPath path) {
      return vectorContainer.getValueVectorId(path);
    }

    @Override
    public VectorWrapper<?> getValueAccessorById(Class<?> clazz, int... fieldIds) {
      return vectorContainer.getValueAccessorById(clazz, fieldIds);
    }

    @Override
    public Iterator<VectorWrapper<?>> iterator() {
      return vectorContainer.iterator();
    }

    public WritableBatch getWritableBatch() {
      return WritableBatch.getBatchNoHVWrap(recordCount, this, false);
    }

    public void clear(){
      vectorContainer.clear();
    }

  }
}
