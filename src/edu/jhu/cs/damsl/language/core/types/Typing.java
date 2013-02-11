package edu.jhu.cs.damsl.language.core.types;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.cs.damsl.catalog.Defaults;
import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.language.interpreter.ExpressionException;

public class Typing {

  public static Logger logger = LoggerFactory.getLogger("typing");
  
  public static Type getType(String typeName) throws ClassNotFoundException
  {
    Type r = null;
    if (typeName.equals("int") || typeName.equals("integer"))
      r = new IntType();
    else if (typeName.equals("long") || typeName.equals("bigint"))
      r = new LongType();
    else if (typeName.equals("float") || typeName.equals("real"))
      r = new FloatType();
    else if (typeName.equals("double"))
      r = new DoubleType();
    else if (typeName.equals("string") || typeName.equals("text"))
      r = new StringType();
    else if (typeName.equals("bool") || typeName.equals("boolean"))
      r = new BooleanType();
    else throw new
      ClassNotFoundException("invalid "+Defaults.systemName+" type: "+typeName);
    return r;
  }
}
