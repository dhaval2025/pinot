/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.pql.parsers.pql2.ast;

import com.linkedin.pinot.common.request.AggregationInfo;


/**
 * TODO Document me!
 *
 * @author jfim
 */
public class FunctionCallAstNode extends AstNode {
  private final String _name;

  public FunctionCallAstNode(String name) {
    _name = name;
  }

  public String getName() {
    return _name;
  }

  public AggregationInfo buildAggregationInfo() {
    String identifier = null;
    for (AstNode astNode : getChildren()) {
      if (astNode instanceof IdentifierAstNode) {
        IdentifierAstNode node = (IdentifierAstNode) astNode;
        if (identifier == null) {
          identifier = node.getName();
        } else {
          throw new AssertionError("Don't know how to proceed");
        }
      } else  if (astNode instanceof StarExpressionAstNode) {
        identifier = "*";
      } else if (astNode instanceof StringLiteralAstNode) {
        // Pinot quirk: Passing a string as an aggregation function is probably a column name
        StringLiteralAstNode node = (StringLiteralAstNode) astNode;
        identifier = node.getText();
      } else {
        throw new AssertionError("Don't know how to proceed");
      }
    }

    String function = _name;

    // Pinot quirk: count is always count(*) no matter what
    if ("count".equalsIgnoreCase(function)) {
      function = "count";
      identifier = "*";
    }

    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(function);
    aggregationInfo.putToAggregationParams("column", identifier);

    return aggregationInfo;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("FunctionCallAstNode{");
    sb.append("_name='").append(_name).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
