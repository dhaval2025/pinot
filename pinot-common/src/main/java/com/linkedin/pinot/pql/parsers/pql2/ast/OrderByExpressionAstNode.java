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
