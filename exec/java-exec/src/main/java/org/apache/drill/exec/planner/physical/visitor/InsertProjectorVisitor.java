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
package org.apache.drill.exec.planner.physical.visitor;

import java.util.Collections;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait.DistributionField;
import org.apache.drill.exec.planner.physical.DeMuxExchangePrel;
import org.apache.drill.exec.planner.physical.ExchangePrel;
import org.apache.drill.exec.planner.physical.HashToRandomExchangePrel;
import org.apache.drill.exec.planner.physical.MuxExchangePrel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.sql.DrillSqlOperator;
import org.eigenbase.rel.RelNode;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexUtil;

import com.google.common.collect.Lists;

/**
 * Visitor to insert additional Exchanges to wrap around HashToRandomExchange to have ability to
 * 1. Merge multiple senders into single (MuxExchange)
 * 2. Do not recalculate Hash by inserting ProjectOperator with additional column to represent hash
 * 3. Insert DeMux exchange
 *
 */
public class InsertProjectorVisitor extends BasePrelVisitor<Prel, Void, RuntimeException> {

  private static InsertProjectorVisitor INSTANCE = new InsertProjectorVisitor();

  public static Prel insertRenameProject(Prel prel){
    return prel.accept(INSTANCE, null);
  }

  @Override
  public Prel visitExchange(ExchangePrel prel, Void value) throws RuntimeException {
    Prel child = prel.iterator().next().accept(this, null);

    if ( !(prel instanceof HashToRandomExchangePrel)) {
      return (Prel)prel.copy(prel.getTraitSet(), Collections.singletonList(((RelNode)child)));
    }

    // Whenever we encounter a HashToRandomExchangePrel, convert it into a tree of MuxExchangePrel,
    // HashToRandomExchangePrel and DemuxExchangePrel.
    HashToRandomExchangePrel hashPrel = (HashToRandomExchangePrel) prel;
    final List<String> childFields = child.getRowType().getFieldNames();

    List<DistributionField> fields = hashPrel.getFields();
    DrillSqlOperator sqlOpH = new DrillSqlOperator("hash", 1, MajorType.getDefaultInstance());
    DrillSqlOperator sqlOpX = new DrillSqlOperator("xor", 2, MajorType.getDefaultInstance());
    RexNode prevRex = null;
    List<String> outputFieldNames = Lists.newArrayList(childFields);
    for ( DistributionField field : fields) {
      RexNode rex = prel.getCluster().getRexBuilder().makeInputRef(child.getRowType().getFieldList().get(field.getFieldId()).getType(), field.getFieldId());
      RexNode rexFunc = prel.getCluster().getRexBuilder().makeCall(sqlOpH, rex);
      if ( prevRex != null ) {
        rexFunc = prel.getCluster().getRexBuilder().makeCall(sqlOpX, prevRex, rexFunc);
      }
      prevRex = rexFunc;
    }
    List <RexNode> updatedExpr = Lists.newArrayList();
    List <RexNode> removeUpdatedExpr = Lists.newArrayList();
    for ( RelDataTypeField field : child.getRowType().getFieldList()) {
      RexNode rex = prel.getCluster().getRexBuilder().makeInputRef(field.getType(), field.getIndex());
      updatedExpr.add(rex);
      removeUpdatedExpr.add(rex);
    }
    outputFieldNames.add("EXPRHASH");

    updatedExpr.add(prevRex);
    RelDataType rowType = RexUtil.createStructType(prel.getCluster().getTypeFactory(), updatedExpr, outputFieldNames);

    ProjectPrel addColumnprojectPrel = new ProjectPrel(child.getCluster(), child.getTraitSet(), child, updatedExpr, rowType);

    Prel localExPrel = new MuxExchangePrel(prel.getCluster(), prel.getTraitSet(), addColumnprojectPrel);

    HashToRandomExchangePrel newHashPrel = (HashToRandomExchangePrel) prel.copy(prel.getTraitSet(), Collections.singletonList( (RelNode) localExPrel ));

    // Insert a DeMuxExchange to narrow down the number of receivers
    DeMuxExchangePrel deMuxExchangePrel = new DeMuxExchangePrel(prel.getCluster(), prel.getTraitSet(),
        newHashPrel, newHashPrel.getFields());

    RelDataType removeRowType = RexUtil.createStructType(deMuxExchangePrel.getCluster().getTypeFactory(), removeUpdatedExpr, childFields);

    ProjectPrel removeColumnProjectPrel = new ProjectPrel(deMuxExchangePrel.getCluster(), deMuxExchangePrel.getTraitSet(), deMuxExchangePrel, removeUpdatedExpr, removeRowType);

    return (Prel) removeColumnProjectPrel.copy(removeColumnProjectPrel.getTraitSet(), Collections.singletonList( (RelNode) deMuxExchangePrel ));
  }

  @Override
  public Prel visitPrel(Prel prel, Void value) throws RuntimeException {
    List<RelNode> children = Lists.newArrayList();
    for(Prel child : prel){
      children.add(child.accept(this, null));
    }
    return (Prel) prel.copy(prel.getTraitSet(), children);
  }
}
