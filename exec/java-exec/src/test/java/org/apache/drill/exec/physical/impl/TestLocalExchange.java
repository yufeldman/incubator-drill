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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.PlanTestBase;
import org.apache.drill.exec.physical.base.Exchange;
import org.apache.drill.exec.physical.config.DeMuxExchange;
import org.apache.drill.exec.physical.config.HashToRandomExchange;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.planner.fragment.Fragment;
import org.apache.drill.exec.planner.fragment.Fragment.ExchangeFragmentPair;
import org.apache.drill.exec.planner.fragment.PlanningSet;
import org.apache.drill.exec.planner.fragment.SimpleParallelizer;
import org.apache.drill.exec.pop.PopUnitTestBase;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionList;
import org.apache.drill.exec.work.QueryWorkUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.PrintWriter;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
public class TestLocalExchange extends PlanTestBase {
  private final static int CLUSTER_SIZE = 3;
  private final static String MUX_EXCHANGE = "\"mux-exchange\"";
  private final static String DEMUX_EXCHANGE = "\"demux-exchange\"";
  private final static UserSession USER_SESSION = UserSession.Builder
      .newBuilder()
      .withCredentials(UserBitShared.UserCredentials.newBuilder().setUserName("foo").build())
      .build();
  private static final SimpleParallelizer PARALLELIZER = new SimpleParallelizer(
      1 /*parallelizationThreshold (slice_count)*/,
      6 /*maxWidthPerNode*/,
      1000 /*maxGlobalWidth*/,
      1.2 /*affinityFactor*/);



  private final static int NUM_DEPTS = 30;
  private final static int NUM_EMPLOYEES = 1000;

  private static String empTableLocation;
  private static String deptTableLocation;

  @BeforeClass
  public static void setupClusterSize() {
    setDrillbitCount(CLUSTER_SIZE);
  }

  /**
   * Generate data for two tables. Each table consists of several JSON files.
   */
  @BeforeClass
  public static void generateTestData() throws Exception {
    // Table 1 consists of two columns "emp_id", "emp_name" and "dept_id"
    File table1 = Files.createTempDir();
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
    File table2 = Files.createTempDir();
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

  public static void setupHelper(boolean isMuxOn, boolean isDeMuxOn) throws Exception {
    // set slice count to 1, so that we can have more parallelization for testing
    test("ALTER SESSION SET `planner.slice_target`=1");
    test("ALTER SESSION SET `planner.enable_mux_exchange`=" + isMuxOn);
    test("ALTER SESSION SET `planner.enable_demux_exchange`=" + isDeMuxOn);
  }

  @Test
  public void testGroupBy_NoMux_NoDeMux() throws Exception {
    testGroupByHelper(false, false);
  }

  @Test
  public void testJoin_NoMux_NoDeMux() throws Exception {
    testJoinHelper(false, false);
  }

  @Test
  public void testGroupBy_Mux_NoDeMux() throws Exception {
    testGroupByHelper(true, false);
  }

  @Test
  public void testJoin_Mux_NoDeMux() throws Exception {
    testJoinHelper(true, false);
  }

  @Test
  public void testGroupBy_NoMux_DeMux() throws Exception {
    testGroupByHelper(false, true);
  }

  @Test
  public void testJoin_NoMux_DeMux() throws Exception {
    testJoinHelper(false, true);
  }

  @Test
  public void testGroupBy_Mux_DeMux() throws Exception {
    testGroupByHelper(true, true);
  }

  @Test
  public void testJoin_Mux_DeMux() throws Exception {
    testJoinHelper(true, true);
  }

  public void testGroupByHelper(boolean isMuxOn, boolean isDeMuxOn) throws Exception {
    setupHelper(isMuxOn, isDeMuxOn);

    String query = String.format("SELECT dept_id, count(*) FROM dfs.`%s` GROUP BY dept_id", empTableLocation);

    String plan = getPlanInString("EXPLAIN PLAN FOR " + query, JSON_FORMAT);
    System.out.println("Plan: " + plan);

    // Make sure the plan has mux and demux exchanges (TODO: currently testing is rudimentary,
    // need to move it to sophisticated testing once we have better planning test tools are available)
    if (isMuxOn) {
      assertTrue("MuxExchange is expected but not present in the plan", plan.contains(MUX_EXCHANGE));
    } else {
      assertFalse("MuxExchange is not expected but present in the plan", plan.contains(MUX_EXCHANGE));
    }

    if (isDeMuxOn) {
      assertTrue("DeMuxExchange is expected but not present in the plan", plan.contains(DEMUX_EXCHANGE));
    } else {
      assertFalse("DeMuxExchange is not expected but present in the plan", plan.contains(DEMUX_EXCHANGE));
    }

    assertEquals(NUM_DEPTS, testSql(query));

    testHelperVerifyPartitionSenderParallelization(plan, isMuxOn, isDeMuxOn);
  }

  public void testJoinHelper(boolean isMuxOn, boolean isDeMuxOn) throws Exception {
    setupHelper(isMuxOn, isDeMuxOn);

    String query =
        String.format("SELECT e.emp_name, d.dept_name FROM dfs.`%s` e JOIN dfs.`%s` d ON e.dept_id = d.dept_id",
            empTableLocation, deptTableLocation);

    String plan = getPlanInString("EXPLAIN PLAN FOR " + query, JSON_FORMAT);
    System.out.println("Plan: " + plan);

    // Make sure the plan has mux and demux exchanges (TODO: currently testing is rudimentary,
    // need to move it to sophisticated testing once we have better planning test tools are available)
    final int expectedNumMuxExchanges = isMuxOn ? 2 : 0;
    assertEquals("Wrong number of MuxExchanges are present in the plan",
        expectedNumMuxExchanges, StringUtils.countMatches(plan, "\"mux-exchange\""));

    final int expectedNumDeMuxExchanges = isDeMuxOn ? 2 : 0;
    assertEquals("Wrong number of DeMuxExchanges are present in the plan",
        expectedNumDeMuxExchanges, StringUtils.countMatches(plan, "\"demux-exchange\""));

    assertEquals(NUM_EMPLOYEES, testSql(query));

    testHelperVerifyPartitionSenderParallelization(plan, isMuxOn, isDeMuxOn);
  }

  // Verify the number of partition senders in a major fragments is not more than the cluster size and each endpoint
  // in the cluster has at most one fragment from a given major fragment that has the partition sender.
  private static void testHelperVerifyPartitionSenderParallelization(
      String plan, boolean isMuxOn, boolean isDeMuxOn) throws Exception {

    final DrillbitContext drillbitContext = getDrillbitContext();
    final PhysicalPlanReader planReader = drillbitContext.getPlanReader();
    final Fragment rootFragment = PopUnitTestBase.getRootFragmentFromPlanString(planReader, plan);

    final List<Integer> deMuxFragments = Lists.newLinkedList();
    final List<Integer> htrFragments = Lists.newLinkedList();
    final PlanningSet planningSet = new PlanningSet();

    // Create a planningSet to get the assignment of major fragment ids to fragments.
    PARALLELIZER.initFragmentWrappers(rootFragment, planningSet);

    findFragmentsWithPartitionSender(rootFragment, planningSet, deMuxFragments, htrFragments);

    QueryWorkUnit qwu = PARALLELIZER.getFragments(new OptionList(), drillbitContext.getEndpoint(),
        QueryId.getDefaultInstance(),
        drillbitContext.getBits(), planReader, rootFragment, USER_SESSION);

    // Make sure the number of minor fragments with HashPartitioner within a major fragment is not more than the
    // number of Drillbits in cluster
    ArrayListMultimap<Integer, DrillbitEndpoint> partitionSenderMap = ArrayListMultimap.create();
    for(PlanFragment planFragment : qwu.getFragments()) {
      if (planFragment.getFragmentJson().contains("hash-partition-sender")) {
        int majorFragmentId = planFragment.getHandle().getMajorFragmentId();
        DrillbitEndpoint assignedEndpoint = planFragment.getAssignment();
        partitionSenderMap.get(majorFragmentId).add(assignedEndpoint);
      }
    }

    if (isMuxOn) {
      verifyAssignment(htrFragments, partitionSenderMap);
    }

    if (isDeMuxOn) {
      verifyAssignment(deMuxFragments, partitionSenderMap);
    }
  }

  /**
   * Helper method to find the major fragment ids of fragments that have PartitionSender.
   * A fragment can have PartitionSender if sending exchange of the current fragment is a
   *   1. DeMux Exchange -> goes in deMuxFragments
   *   2. HashToRandomExchange -> goes into htrFragments
   */
  private static void findFragmentsWithPartitionSender(Fragment currentRootFragment, PlanningSet planningSet,
      List<Integer> deMuxFragments, List<Integer> htrFragments) {

    if (currentRootFragment != null) {
      final Exchange sendingExchange = currentRootFragment.getSendingExchange();
      if (sendingExchange != null) {
        final int majorFragmentId = planningSet.get(currentRootFragment).getMajorFragmentId();
        if (sendingExchange instanceof DeMuxExchange) {
          deMuxFragments.add(majorFragmentId);
        } else if (sendingExchange instanceof HashToRandomExchange) {
          htrFragments.add(majorFragmentId);
        }
      }

      for(ExchangeFragmentPair e : currentRootFragment.getReceivingExchangePairs()) {
        findFragmentsWithPartitionSender(e.getNode(), planningSet, deMuxFragments, htrFragments);
      }
    }
  }

  /** Helper method to verify the number of PartitionSenders in a given fragment endpoint assignments */
  private static void verifyAssignment(List<Integer> fragmentList,
      ArrayListMultimap<Integer, DrillbitEndpoint> partitionSenderMap) {

    // We expect at least one entry the list
    assertTrue(fragmentList.size() > 0);

    for(Integer majorFragmentId : fragmentList) {
      // we expect the fragment that has DeMux/HashToRandom as sending exchange to have parallelization with not more
      // than the number of nodes in the cluster and each node in the cluster can have at most one assignment
      List<DrillbitEndpoint> assignments = partitionSenderMap.get(majorFragmentId);
      assertTrue(assignments != null && assignments.size() > 0);
      assertTrue(String.format("Number of partition senders in major fragment [%d] is more than expected", majorFragmentId), CLUSTER_SIZE >= assignments.size());

      // Make sure there are no duplicates in assigned endpoints (i.e at most one partition sender per endpoint)
      assertTrue("Some endpoints have more than one fragment that has ParitionSender", ImmutableSet.copyOf(assignments).size() == assignments.size());
    }
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