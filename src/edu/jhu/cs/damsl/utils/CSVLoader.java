package edu.jhu.cs.damsl.utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;

import edu.jhu.cs.damsl.engine.dbms.DbEngine;
import edu.jhu.cs.damsl.engine.storage.StorageEngine;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.transactions.TransactionAbortException;
import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.TableId;
import edu.jhu.cs.damsl.language.core.types.Type;

public class CSVLoader {
  private int lineNumber = 0;
  private int tokenNumber = 0;
  private String strLine = "";
  private StringTokenizer stringTokenizer = null;
  private String filename = "";

  public CSVLoader(String filename) {
    this.filename = filename;
  }

  public void load(DbEngine dbms, TableId tid, Schema schema) {
    try {
      // csv file containing data
      BufferedReader br = new BufferedReader(new FileReader(filename));     
      
      // read pipe separated file line by line
      while((strLine = br.readLine()) != null) {
        lineNumber++;
        List<String> values = new ArrayList<String>(Arrays.asList(strLine.split("\\|")));
        List<Object> fields = new ArrayList<Object>();

        Iterator<String> it1 = values.iterator();
        Iterator<Type> it2 = schema.getTypes().iterator();
        Iterator<String> it3 = schema.getFields().iterator();

        while (it1.hasNext() && it2.hasNext()) {
          String s = it1.next();
          Type t = it2.next();
          String f = it3.next();
          fields.add(t.parseType(s));
        }

        if ( (lineNumber % 1000) == 0 ) {
          System.out.println("Read " + Integer.toString(lineNumber / 1000) + "k tuples");
        }

        Tuple t = Tuple.schemaTuple(schema, fields);
        dbms.getStorageEngine().insertTuple(null, tid, t);
      }

    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (TransactionAbortException e) {
      e.printStackTrace();
    }
  }
}
