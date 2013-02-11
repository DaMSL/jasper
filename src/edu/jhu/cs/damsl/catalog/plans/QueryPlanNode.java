package edu.jhu.cs.damsl.catalog.plans;

import java.io.Serializable;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.cs.damsl.catalog.NameAddress;
import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.language.core.types.TypeException;
import edu.jhu.cs.damsl.language.core.types.Typeable;
import edu.jhu.cs.damsl.language.interpreter.ExpressionException;

public abstract class QueryPlanNode extends NameAddress
                                    implements Typeable, Serializable
{
  protected static final Logger logger = LoggerFactory.getLogger(QueryPlanNode.class);
  protected Schema schema;

  public QueryPlanNode(String name) { super(name); }
  
  public QueryPlanNode(String name, Schema s) {
    super(name);
    schema = s;
  }
 
  // Override equals, hashCode for structural equality on plans.
  @Override
  public boolean equals(Object other) {
    if ( other != null && other instanceof QueryPlanNode ) {
      return getAddress() == ((QueryPlanNode) other).getAddress();
    }
    return false;
  }
  
  @Override
  public int hashCode() { return getAddress(); }
  
  public abstract boolean isStream();
  
  public abstract boolean isOperator();

  // Schema accessors
  public void setSchema(Schema s) { schema = s; }

  public boolean hasSchema() { return schema != null; }

  public Schema getSchema() throws TypeException {
    if ( schema == null ) throw new TypeException();
    return schema;
  }

  public Schema infer(List<Schema> inputSchemas)
      throws ExpressionException, TypeException
  {
    if ( schema == null && inputSchemas.size() > 0 )
      schema = inputSchemas.get(0);
    return getSchema();
  }  
}
