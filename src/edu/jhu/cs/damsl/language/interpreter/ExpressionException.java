package edu.jhu.cs.damsl.language.interpreter;

public class ExpressionException extends Exception {

  private static final long serialVersionUID = -4720589027395563906L;
  public ExpressionException() {}
  public ExpressionException(String msg) { super(msg); }
}
