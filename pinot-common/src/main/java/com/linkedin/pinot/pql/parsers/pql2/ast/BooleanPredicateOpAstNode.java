package com.linkedin.pinot.pql.parsers.pql2.ast;

import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import java.util.ArrayList;
import java.util.List;


/**
 * TODO Document me!
 *
 * @author jfim
 */
public class BooleanPredicateOpAstNode extends PredicateAstNode {
  private String _operand;

  public BooleanPredicateOpAstNode(String operand) {
    _operand = operand;
  }

  @Override
  public void doneProcessingChildren() {
    for (AstNode childNode : getChildrenCopy()) {
      // If the child node is of the same type as us, merge its children nodes with ours
      if (childNode instanceof BooleanPredicateOpAstNode) {
        BooleanPredicateOpAstNode node = (BooleanPredicateOpAstNode) childNode;
        if (node.getOperand().equalsIgnoreCase(_operand)) {
          // Reparent children nodes
          for (AstNode astNode : node.getChildrenCopy()) {
            astNode.reparent(this);
          }
          // Detach child op from the tree
          node.reparent(null);
        }
      }
    }
  }

  public String getOperand() {
    return _operand;
  }

  @Override
  public FilterQueryTree buildFilterQueryTree() {
    // Build children query trees
    List<FilterQueryTree> childrenQueryTrees = new ArrayList<FilterQueryTree>();
    for (AstNode astNode : getChildren()) {
      if (astNode instanceof PredicateAstNode) {
        PredicateAstNode node = (PredicateAstNode) astNode;
        childrenQueryTrees.add(node.buildFilterQueryTree());
      } else {
        throw new AssertionError("Got non predicate ast node");
      }
    }

    FilterOperator operator;
    if ("and".equalsIgnoreCase(_operand)) {
      operator = FilterOperator.AND;
    } else if ("or".equalsIgnoreCase(_operand)) {
      operator = FilterOperator.OR;
    } else {
      throw new AssertionError("Don't know how to handle operand of type " + _operand);
    }

    return new FilterQueryTree(null, null, operator, childrenQueryTrees);
  }

  @Override
  public String toString() {
    return "BooleanPredicateOpAstNode{" +
        "_operand='" + _operand + '\'' +
        '}';
  }
}
