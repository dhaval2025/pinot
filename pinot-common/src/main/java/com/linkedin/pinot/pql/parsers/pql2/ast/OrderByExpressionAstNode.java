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

/**
 * FIXME Document me!
 *
 * @author jfim
 */
public class OrderByExpressionAstNode extends AstNode {
  private String _column;
  private String _ordering;

  public OrderByExpressionAstNode(String column, String ordering) {
    _column = column;
    _ordering = ordering;
  }

  public String getColumn() {
    return _column;
  }

  public String getOrdering() {
    return _ordering;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("OrderByExpressionAstNode{");
    sb.append("_column='").append(_column).append('\'');
    sb.append(", _ordering='").append(_ordering).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
