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

import java.util.ArrayList;
import java.util.List;


/**
 * TODO Document me!
 *
 * @author jfim
 */
public abstract class AstNode {
  private List<AstNode> _children;
  private AstNode _parent;

  public List<AstNode> getChildren() {
    return _children;
  }

  public boolean hasChildren() {
    return _children != null && !_children.isEmpty();
  }

  public void addChild(AstNode childNode) {
    if (_children == null) {
      _children = new ArrayList<AstNode>();
    }
    _children.add(childNode);
  }

  public void seal() {
  }

  public AstNode getParent() {
    return _parent;
  }

  public boolean hasParent() {
    return _parent != null;
  }

  public String toString(int indent) {
    String str = "";
    for(int i = 0; i < indent; ++i) {
      str += " ";
    }
    str += toString();
    if (hasChildren()) {
      for (AstNode child : _children) {
        str += "\n" + child.toString(indent + 2);
      }
    }
    return str;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
