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
package org.apache.drill.exec.physical.impl;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.planner.fragment.Fragment;
import org.apache.drill.exec.planner.fragment.SimpleParallelizer;
import org.apache.drill.exec.pop.PopUnitTestBase;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.server.options.OptionList;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.work.QueryWorkUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
/**
 * This test starts a Drill cluster with CLUSTER_SIZE nodes and generates data for test tables.
 *
 * Tests queries involve HashToRandomExchange (group by and join) and test the following.
 *   1. Plan that has mux and demux exchanges inserted
 *   2. Run the query and check the output record count
 *   3. Take the plan we got in (1), use SimpleParallelizer to get PlanFragments and test that the number of
 *   partition senders in a major fragment is not more than the number of Drillbit nodes in cluster and there exists
 *   at most one partition sender per Drillbit.
 */
public class TestLocalExchange extends PopUnitTestBase {

  private final static int CLUSTER_SIZE = 3;

  private final static int NUM_DEPTS = 30;
  private final static int NUM_EMPLOYEES = 1000;

  private static String empTableLocation = null;
  private static String deptTableLocation = null;

  private static RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
  private static List<Drillbit> drillbits = Lists.newArrayList();
  private static DrillClient client = null;

  @BeforeClass
  public static void generateTestData() throws Exception {
    // Generate data for two tables. Each table consists of several JSON files.

    // Table 1 consists of two columns "emp_id", "emp_name" and "dept_id"
    File table1 = getTempFile();
    table1.mkdirs();
    empTableLocation = table1.getAbsolutePath();

    // Write 100 records for each new file
    for(int fileIndex=0; fileIndex<NUM_EMPLOYEES/100; fileIndex++) {
      File file = new File(empTableLocation + File.separator + fileIndex + ".json");
      PrintWriter printWriter = new PrintWriter(file);
      for (int recordIndex = fileIndex*100; recordIndex < (fileIndex+1)*100; recordIndex++) {
        String record = String.format("{ \"emp_id\" : %d, \"emp_name\" : \"Employee %d\", \"dept_id\" : %d }",
            recordIndex, recordIndex, recordIndex % NUM_DEPTS);
        printWriter.println(record);
      }
      printWriter.close();
    }

    // Table 2 consists of two columns "dept_id" and "dept_name"
    File table2 = getTempFile();
    table2.mkdirs();
    deptTableLocation = table2.getAbsolutePath();

    // Write 3 records for each new file
    for(int fileIndex=0; fileIndex<NUM_DEPTS/3; fileIndex++) {
      File file = new File(deptTableLocation + File.separator + fileIndex + ".json");
      PrintWriter printWriter = new PrintWriter(file);
      for (int recordIndex = fileIndex*3; recordIndex < (fileIndex+1)*3; recordIndex++) {
        String record = String.format("{ \"dept_id\" : %d, \"dept_name\" : \"Department %d\" }",
            recordIndex, recordIndex);
        printWriter.println(record);
      }
      printWriter.close();
    }
  }

  private static File getTempFile() throws Exception {
    File file;
    while (true) {
      file = File.createTempFile("test", "");
      if (file.exists()) {
        boolean success = file.delete();
        if (success) {
          break;
        }
      }
    }

    return file;
  }

  @BeforeClass
  public static void startDrillCluster() throws Exception {
    // Add three Drillbits to cluster and open a client connection.
    for(int i=0; i<CLUSTER_SIZE; i++) {
      Drillbit drillbit = new Drillbit(CONFIG, serviceSet);
      drillbit.run();
      drillbits.add(drillbit);
    }
    client = new DrillClient(CONFIG, serviceSet.getCoordinator());
    client.connect();

    // set slice count to 1, so that we can have more parallelization for testing
    List<QueryResultBatch> results = client.runQuery(QueryType.SQL, "ALTER SESSION SET `planner.slice_target`=1");
    for (QueryResultBatch b : results) {
      b.release();
    }
  }

  @Test
  public void testGroupBy() throws Exception {
    String query = String.format("SELECT dept_id, count(*) FROM dfs.`%s` GROUP BY dept_id", empTableLocation);

    String plan = getQueryPlan(query);
    System.out.println("Plan: " + plan);

    // Make sure the plan has mux and demux exchanges (TODO: currently testing is rudimentary,
    // need to move it to sophisticated testing once we have better planning test tools are available)
    assertTrue("MuxExchange is not present in the plan", plan.contains("mux-exchange"));
    assertTrue("DeMuxExchange is not present in the plan", plan.contains("demux-exchange"));

    assertEquals(NUM_DEPTS, runQuery(query));

    testHelperVerifyPartitionSenderParallelization(plan);
  }

  @Test
  public void testJoin() throws Exception {
    String query =
        String.format("SELECT e.emp_name, d.dept_name FROM dfs.`%s` e JOIN dfs.`%s` d ON e.dept_id = d.dept_id",
            empTableLocation, deptTableLocation);

    String plan = getQueryPlan(query);
    System.out.println("Plan: " + plan);

    // Make sure the plan has mux and demux exchanges (TODO: currently testing is rudimentary,
    // need to move it to sophisticated testing once we have better planning test tools are available)
    assertEquals("Wrong number of MuxExchanges are present in the plan",
        2, StringUtils.countMatches(plan, "\"mux-exchange\""));
    assertEquals("Wrong number of DeMuxExchanges are present in the plan",
        2, StringUtils.countMatches(plan, "\"demux-exchange\""));

    assertEquals(NUM_EMPLOYEES, runQuery(query));

    testHelperVerifyPartitionSenderParallelization(plan);
  }

  // Helper method which returns the plan for given query.
  private static String getQueryPlan(String query) throws Exception {
    String planQuery = "EXPLAIN PLAN FOR " + query;

    List<QueryResultBatch> results = client.runQuery(QueryType.SQL, planQuery);

    RecordBatchLoader loader = new RecordBatchLoader(drillbits.get(0).getContext().getAllocator());
    StringBuilder builder = new StringBuilder();

    for (QueryResultBatch b : results) {
      if (!b.hasData()) {
        continue;
      }

      loader.load(b.getHeader().getDef(), b.getData());

      VectorWrapper<?> vw = loader.getValueAccessorById(
          NullableVarCharVector.class, //
          loader.getValueVectorId(SchemaPath.getSimplePath("json")).getFieldIds()
      );

      ValueVector vv = vw.getValueVector();
      for (int i = 0; i < vv.getAccessor().getValueCount(); i++) {
        Object o = vv.getAccessor().getObject(i);
        builder.append(o);
      }
      loader.clear();
      b.release();
    }

    return builder.toString();
  }

  // Verify the number of partition senders in a major fragments is not more than the cluster size and each endpoint
  // in the cluster has at most one fragment from a given major fragment that has the partition sender.
  private static void testHelperVerifyPartitionSenderParallelization(String plan) throws Exception {
    DrillbitContext drillbitContext = drillbits.get(0).getContext();
    PhysicalPlanReader planReader = drillbitContext.getPlanReader();
    Fragment rootFragment = getRootFragmentFromPlanString(planReader, plan);
    UserSession userSession = UserSession.Builder
        .newBuilder()
        .withCredentials(UserBitShared.UserCredentials.newBuilder().setUserName("foo").build())
        .build();

    SimpleParallelizer par = new SimpleParallelizer(
        1 /*parallelizationThreshold (slice_count)*/,
        6 /*maxWidthPerNode*/,
        1000 /*maxGlobalWidth*/,
        1.2 /*affinityFactor*/);

    QueryWorkUnit qwu = par.getFragments(new OptionList(), drillbitContext.getEndpoint(), QueryId.getDefaultInstance(),
        drillbitContext.getBits(), planReader, rootFragment, userSession);

    // Make sure the number of minor fragments with HashPartitioner within a major fragment is not more than the
    // number of Drillbits in cluster
    Map<Integer, List<DrillbitEndpoint>> partitionSenderMap = Maps.newHashMap();
    for(PlanFragment planFragment : qwu.getFragments()) {
      if (planFragment.getFragmentJson().contains("hash-partition-sender")) {
        int majorFragmentId = planFragment.getHandle().getMajorFragmentId();
        DrillbitEndpoint assignedEndpoint = planFragment.getAssignment();
        if (partitionSenderMap.containsKey(majorFragmentId)) {
          partitionSenderMap.get(majorFragmentId).add(assignedEndpoint);
        } else {
          partitionSenderMap.put(majorFragmentId, Lists.newArrayList(assignedEndpoint));
        }
      }
    }

    for(Entry<Integer, List<DrillbitEndpoint>> entry : partitionSenderMap.entrySet()) {
      assertTrue(String.format("Number of partition senders in major fragment [%d] is more than expected",
          entry.getKey()), CLUSTER_SIZE >= entry.getValue().size());
      // Make sure there are no duplicates in assigned endpoints (i.e at most one partition sender per endpoint)
      assertTrue("Some endpoints have more than one ParitionSender",
          ImmutableSet.copyOf(entry.getValue()).size() == entry.getValue().size());
    }
  }

  // Helper method which executes the given query and returns number of records in output
  private int runQuery(String query) throws Exception {
    List<QueryResultBatch> results = client.runQuery(QueryType.SQL, query);
    int count = 0;
    for (QueryResultBatch b : results) {
      count += b.getHeader().getRowCount();
      b.release();
    }

    return count;
  }

  @AfterClass
  public static void shutdownDrillCluster() throws Exception {
    if (client != null) {
      client.close();
    }

    for(Drillbit drillbit : drillbits) {
      drillbit.close();
    }

    serviceSet.close();
  }

  @AfterClass
  public static void cleanupTestData() throws Exception {
    File f = new File(empTableLocation);
    if(f.exists()){
      FileUtils.forceDelete(f);
    }

    f = new File(deptTableLocation);
    if(f.exists()){
      FileUtils.forceDelete(f);
    }
  }
}