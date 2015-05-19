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
package com.linkedin.pinot.pql.parsers.pql2;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.QuerySource;


/**
 * TODO Document me!
 *
 * @author jfim
 */
public class SelectAstNode extends AstNode {
  private String _tableName;
  private String _resourceName;

  public SelectAstNode() {
  }

  @Override
  public void addChild(AstNode childNode) {
    if (childNode instanceof TableNameAstNode) {
      TableNameAstNode node = (TableNameAstNode) childNode;
      _tableName = node.getTableName();
      _resourceName = node.getResourceName();
    } else {
      super.addChild(childNode);
    }
  }

  @Override
  public String toString() {
    return "SelectAstNode{" +
        "_tableName='" + _tableName + '\'' +
        ", _resourceName='" + _resourceName + '\'' +
        '}';
  }

  @Override
  public void updateBrokerRequest(BrokerRequest brokerRequest) {
    QuerySource querySource = new QuerySource();
    querySource.setResourceName(_resourceName);
    querySource.setTableName(_tableName);
    brokerRequest.setQuerySource(querySource);
    sendBrokerRequestUpdateToChildren(brokerRequest);
  }
}
