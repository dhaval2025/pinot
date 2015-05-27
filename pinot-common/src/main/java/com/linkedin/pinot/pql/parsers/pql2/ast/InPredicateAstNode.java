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

import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import java.util.Collections;
import java.util.TreeSet;


/**
 * TODO Document me!
 *
 * @author jfim
 */
public class InPredicateAstNode extends PredicateAstNode {
  private final boolean _isNotInClause;
  private String _identifier;
  private static final boolean SINGLE_VALUE_IN_TO_EQUALITY = false;

  public InPredicateAstNode(boolean isNotInClause) {
    _isNotInClause = isNotInClause;
  }

  @Override
  public void addChild(AstNode childNode) {
    if (childNode instanceof IdentifierAstNode) {
      if (_identifier == null) {
        IdentifierAstNode node = (IdentifierAstNode) childNode;
        _identifier = node.getName();
      } else {
        throw new AssertionError("Don't know how to handle this node");
      }
    } else {
      super.addChild(childNode);
    }
  }

  public String getIdentifier() {
    return _identifier;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("InPredicateAstNode{");
    sb.append("_identifier='").append(_identifier).append('\'');
    sb.append('}');
    return sb.toString();
  }

  @Override
  public FilterQueryTree buildFilterQueryTree() {
    if (_identifier == null) {
      throw new AssertionError("Identifier is null!");
    }

    TreeSet<String> values = new TreeSet<String>();

    for (AstNode astNode : getChildren()) {
      if (astNode instanceof LiteralAstNode) {
        LiteralAstNode node = (LiteralAstNode) astNode;
        values.add(node.getValueAsString());
      }
    }

    if (1 < values.size() || !SINGLE_VALUE_IN_TO_EQUALITY) {
      String[] valueArray = values.toArray(new String[values.size()]);
      FilterOperator filterOperator;
      if (_isNotInClause) {
        filterOperator = FilterOperator.NOT_IN;
      } else {
        filterOperator = FilterOperator.IN;
      }
      return new FilterQueryTree(_identifier, Collections.singletonList(StringUtil.join("\t\t", valueArray)),
          filterOperator, null);
    } else {
      return new FilterQueryTree(_identifier, Collections.singletonList(values.first()), FilterOperator.EQUALITY, null);
    }
  }
}
