/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.physical.impl.broadcastsender;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;

import java.util.List;

import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.MetricDef;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.MinorFragmentEndpoint;
import org.apache.drill.exec.physical.config.BroadcastSender;
import org.apache.drill.exec.physical.impl.BaseRootExec;
import org.apache.drill.exec.physical.impl.SendingAccountor;
import org.apache.drill.exec.proto.ExecProtos;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.GeneralRPCProtos;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.record.FragmentWritableBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.rpc.BaseRpcOutcomeListener;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.data.DataTunnel;
import org.apache.drill.exec.work.ErrorHelper;

/**
 * Broadcast Sender broadcasts incoming batches to all receivers (one or more).
 * This is useful in cases such as broadcast join where sending the entire table to join
 * to all nodes is cheaper than merging and computing all the joins in the same node.
 */
public class BroadcastSenderRootExec extends BaseRootExec {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BroadcastSenderRootExec.class);
  private final FragmentContext context;
  private final BroadcastSender config;
  private final List<FragmentDataTunnel> tunnels;
  private final ExecProtos.FragmentHandle handle;
  private volatile boolean ok;
  private final RecordBatch incoming;

  private class FragmentDataTunnel {
    public final int minorFragmentId;
    public final DataTunnel dataTunnel;

    public FragmentDataTunnel(int minorFragmentId, DataTunnel dataTunnel) {
      this.minorFragmentId = minorFragmentId;
      this.dataTunnel = dataTunnel;
    }
  }

  public enum Metric implements MetricDef {
    N_RECEIVERS,
    BYTES_SENT;
    @Override
    public int metricId() {
      return ordinal();
    }
  }

  public BroadcastSenderRootExec(FragmentContext context,
                                 RecordBatch incoming,
                                 BroadcastSender config) throws OutOfMemoryException {
    super(context, new OperatorContext(config, context, null, false), config);
    //super(context, config);
    this.ok = true;
    this.context = context;
    this.incoming = incoming;
    this.config = config;
    this.handle = context.getHandle();
    tunnels = Lists.newArrayListWithCapacity(config.getDestinations().size());
    for(MinorFragmentEndpoint destination : config.getDestinations()) {
      FragmentHandle opp = handle.toBuilder()
          .setMajorFragmentId(config.getOppositeMajorFragmentId())
          .setMinorFragmentId(destination.getId())
          .build();
      tunnels.add(new FragmentDataTunnel(destination.getId(), context.getDataTunnel(destination.getEndpoint(), opp)));
    }
  }

  @Override
  public boolean innerNext() {
    if(!ok) {
      context.fail(statusHandler.ex);
      return false;
    }

    RecordBatch.IterOutcome out = next(incoming);
    logger.debug("Outcome of sender next {}", out);
    switch(out){
      case STOP:
      case NONE:
        for (FragmentDataTunnel tunnel : tunnels) {
          FragmentWritableBatch b2 = FragmentWritableBatch.getEmptyLast(
              handle.getQueryId(),
              handle.getMajorFragmentId(),
              handle.getMinorFragmentId(),
              config.getOppositeMajorFragmentId(),
              tunnel.minorFragmentId);
          stats.startWait();
          try {
            tunnel.dataTunnel.sendRecordBatch(statusHandler, b2);
          } finally {
            stats.stopWait();
          }
          statusHandler.sendCount.increment();
        }

        return false;

      case OK_NEW_SCHEMA:
      case OK:
        WritableBatch writableBatch = incoming.getWritableBatch();
        if (tunnels.size() > 1) {
          writableBatch.retainBuffers(tunnels.size() - 1);
        }
        for (FragmentDataTunnel tunnel : tunnels) {
          FragmentWritableBatch batch = new FragmentWritableBatch(
              false,
              handle.getQueryId(),
              handle.getMajorFragmentId(),
              handle.getMinorFragmentId(),
              config.getOppositeMajorFragmentId(),
              tunnel.minorFragmentId,
              writableBatch);
          updateStats(batch);
          stats.startWait();
          try {
            tunnel.dataTunnel.sendRecordBatch(statusHandler, batch);
          } finally {
            stats.stopWait();
          }
          statusHandler.sendCount.increment();
        }

        return ok;

      case NOT_YET:
      default:
        throw new IllegalStateException();
    }
  }

  public void updateStats(FragmentWritableBatch writableBatch) {
    stats.setLongStat(Metric.N_RECEIVERS, tunnels.size());
    stats.addLongStat(Metric.BYTES_SENT, writableBatch.getByteCount());
  }

  /*
  private boolean waitAllFutures(boolean haltOnError) {
    for (DrillRpcFuture<?> responseFuture : responseFutures) {
      try {
        GeneralRPCProtos.Ack ack = (GeneralRPCProtos.Ack) responseFuture.checkedGet();
        if(!ack.getOk()) {
          ok = false;
          if (haltOnError) {
            return false;
          }
        }
      } catch (RpcException e) {
        logger.error("Error sending batch to receiver: " + e);
        ok = false;
        if (haltOnError) {
          return false;
        }
      }
    }
    return true;
  }
*/

  @Override
  public void stop() {
      ok = false;
      statusHandler.sendCount.waitForSendComplete();
      oContext.close();
      incoming.cleanup();
  }

  private StatusHandler statusHandler = new StatusHandler();
  private class StatusHandler extends BaseRpcOutcomeListener<GeneralRPCProtos.Ack> {
    volatile RpcException ex;
    private final SendingAccountor sendCount = new SendingAccountor();

    @Override
    public void success(Ack value, ByteBuf buffer) {
      sendCount.decrement();
      super.success(value, buffer);
    }

    @Override
    public void failed(RpcException ex) {
      sendCount.decrement();
      logger.error("Failure while sending data to user.", ex);
      ErrorHelper.logAndConvertError(context.getIdentity(), "Failure while sending fragment to client.", ex, logger);
      ok = false;
      this.ex = ex;
    }
  }

}
