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
package org.apache.drill.exec.planner.physical.visitor;

import com.google.common.collect.Lists;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.planner.physical.DeMuxExchangePrel;
import org.apache.drill.exec.planner.physical.ExchangePrel;
import org.apache.drill.exec.planner.physical.HashToRandomExchangePrel;
import org.apache.drill.exec.planner.physical.MuxExchangePrel;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.SelectionVectorRemoverPrel;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait.DistributionField;
import org.apache.drill.exec.planner.sql.DrillSqlOperator;
import org.apache.drill.exec.server.options.OptionManager;
import org.eigenbase.rel.RelNode;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexUtil;

import java.util.Collections;
import java.util.List;

public class InsertLocalExchangeVisitor extends BasePrelVisitor<Prel, Void, RuntimeException> {

  private final boolean isMuxEnabled;
  private final boolean isDeMuxEnabled;

  public static Prel insertLocalExchanges(Prel prel, OptionManager options) {
    boolean isMuxEnabled = options.getOption(PlannerSettings.MUX_EXCHANGE.getOptionName()).bool_val;
    boolean isDeMuxEnabled = options.getOption(PlannerSettings.DEMUX_EXCHANGE.getOptionName()).bool_val;

    return prel.accept(new InsertLocalExchangeVisitor(isMuxEnabled, isDeMuxEnabled), null);
  }

  public InsertLocalExchangeVisitor(boolean isMuxEnabled, boolean isDeMuxEnabled) {
    this.isMuxEnabled = isMuxEnabled;
    this.isDeMuxEnabled = isDeMuxEnabled;
  }

  @Override
  public Prel visitExchange(ExchangePrel prel, Void value) throws RuntimeException {
    Prel child = ((Prel)prel.getChild()).accept(this, null);
    // Whenever we encounter a HashToRandomExchangePrel:
    //   If MuxExchange is enabled, insert a MuxExchangePrel before HashToRandomExchangePrel.
    //   If DeMuxExchange is enabled, insert a DeMuxExchangePrel after HashToRandomExchangePrel.
    if (prel instanceof HashToRandomExchangePrel) {

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

      Prel newPrel = addColumnprojectPrel;


      if (isMuxEnabled) {
        // We need a SVRemover, because the sender used in MuxExchange doesn't support selection vector
        //Prel svRemovePrel = new SelectionVectorRemoverPrel(child);
        newPrel = new MuxExchangePrel(prel.getCluster(), prel.getTraitSet(), newPrel);
      }

      newPrel = new HashToRandomExchangePrel(prel.getCluster(),
          prel.getTraitSet(), newPrel, ((HashToRandomExchangePrel) prel).getFields());

      if (isDeMuxEnabled) {
        HashToRandomExchangePrel hashExchangePrel = (HashToRandomExchangePrel) newPrel;
        // Insert a DeMuxExchange to narrow down the number of receivers
        newPrel = new DeMuxExchangePrel(prel.getCluster(), prel.getTraitSet(), hashExchangePrel,
            hashExchangePrel.getFields());
      }

      RelDataType removeRowType = RexUtil.createStructType(newPrel.getCluster().getTypeFactory(), removeUpdatedExpr, childFields);

      ProjectPrel removeColumnProjectPrel = new ProjectPrel(newPrel.getCluster(), newPrel.getTraitSet(), newPrel, removeUpdatedExpr, removeRowType);

      return (Prel) removeColumnProjectPrel.copy(removeColumnProjectPrel.getTraitSet(), Collections.singletonList( (RelNode) newPrel ));
    }

    return (Prel)prel.copy(prel.getTraitSet(), Collections.singletonList(((RelNode)child)));
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
