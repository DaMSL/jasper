package edu.jhu.cs.damsl.language.core.types;

import java.util.List;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.language.interpreter.ExpressionException;

public interface Typeable {
  
  public boolean hasSchema();
  
  public Schema getSchema() throws TypeException;
  
  public void setSchema(Schema s);

  public abstract Schema infer(List<Schema> inputSchemas)
    throws ExpressionException, TypeException;
}
