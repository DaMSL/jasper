package edu.jhu.cs.damsl.catalog.specs;

import java.io.Serializable;
import java.util.List;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.TableId;
import edu.jhu.cs.damsl.catalog.plans.QueryPlanNode;
import edu.jhu.cs.damsl.language.core.types.TypeException;
import edu.jhu.cs.damsl.language.interpreter.ExpressionException;

public class TableSpec extends QueryPlanNode {
  TableId id;

  public TableSpec(String nm, Schema s) {
    super(nm, s);
    id = new TableId(nm);
  }

  public TableId getId() { return id; }
  
  public String getName() { return getAddressString(); }

  // TODO: change for isTable() 
  @Override
  public boolean isStream() { return true; }

  @Override
  public boolean isOperator() { return false; }

  @Override
  public Schema infer(List<Schema> inputSchemas)
      throws ExpressionException, TypeException
  {
    return getSchema();
  }  

}
