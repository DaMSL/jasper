package edu.jhu.cs.damsl.utils;

import java.util.*;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.language.core.types.*;

public class Lineitem {
  public Lineitem() {}
  
  public static Schema getSchema(String tableName) {
    LinkedHashMap<String, Type> fts = new LinkedHashMap<String, Type>();

    fts.put("orderkey", new IntType());
    fts.put("partkey", new IntType());
    fts.put("suppkey", new IntType());
    fts.put("linenumber", new IntType());

    fts.put("quantity", new DoubleType());
    fts.put("extendedprice", new DoubleType());
    fts.put("discount", new DoubleType());

    fts.put("shipdate", new IntType());
    fts.put("commitdate", new IntType());
    fts.put("receiptdate", new IntType());

    return new Schema(tableName, (Map<String, Type>)fts);
  }

}