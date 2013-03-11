package edu.jhu.cs.damsl.utils.hw1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.StringTokenizer;
import java.util.*;

import edu.jhu.cs.damsl.catalog.Catalog;
import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TableId;
import edu.jhu.cs.damsl.catalog.identifiers.tuple.SlottedTupleId;
import edu.jhu.cs.damsl.catalog.specs.TableSpec;
import edu.jhu.cs.damsl.engine.dbms.DbEngine;
import edu.jhu.cs.damsl.engine.storage.file.SlottedHeapFile;
import edu.jhu.cs.damsl.engine.storage.iterator.page.StorageIterator;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPage;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPageHeader;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.factory.file.SlottedStorageFileFactory;
import edu.jhu.cs.damsl.language.core.types.*;
import edu.jhu.cs.damsl.utils.CSVLoader;
import edu.jhu.cs.damsl.utils.Lineitem;
import edu.jhu.cs.damsl.utils.WorkloadGenerator;

public class HW1Terminal extends Thread {
  String prompt;

  DbEngine<SlottedTupleId, SlottedPageHeader, SlottedPage, SlottedHeapFile> dbms;
  WorkloadGenerator<SlottedTupleId, SlottedPageHeader, SlottedPage, SlottedHeapFile> generator;
  
  public HW1Terminal(String prompt) {
    this.prompt = prompt;
    
    SlottedStorageFileFactory f = new SlottedStorageFileFactory();
    dbms = new DbEngine<SlottedTupleId, SlottedPageHeader, SlottedPage, SlottedHeapFile>(f);

    generator = 
      new WorkloadGenerator<SlottedTupleId, SlottedPageHeader, SlottedPage, SlottedHeapFile>(dbms);
  }
  
  public static LinkedList<String> parseCommand(String msg) {
    StringTokenizer tokenizer = new StringTokenizer(msg);
    LinkedList<String> fields = new LinkedList<String>();
    while (tokenizer.hasMoreTokens()) fields.add(tokenizer.nextToken());
    return fields;
  }

  // Helper functions for command line interface.
  public void printHelp() {
    String[] cmds = new String[] {
      "Available commands:",
      "csv <relation name> <file name>",
      "load <catalog file name>",
      "save <catalog file name>",
      "show",
      "page <relation name> <page id>",
      "benchmark <benchmark mode> <relation name> [probability]",
      "bye"
    };
    String help = ""; for (String c : cmds) { help += c+"\n"; }
    System.out.println(help);
  }

  Page getPageFromFile(TableId tId, int fileIdx, int pageNum) {
    Page r = null;
    List<FileId> files = tId.files();
    if ( !( files == null || files.isEmpty() || fileIdx >= files.size() ) ) {
      FileId fid = files.get(fileIdx);
      if ( pageNum < fid.numPages() ) {
        PageId pid = new PageId(fid, pageNum);
        r = dbms.getStorageEngine().getPage(null, pid, Page.Permissions.READ);
      } else {
        System.err.println("Invalid page number " + Integer.toString(pageNum));
      }
    } else if ( files != null && fileIdx >= files.size() ) {
      System.err.println("Invalid file index " + fileIdx);
    } else {
      System.err.println("Empty relation " + tId.name());
    }
    return r;
  }

  void printPage(TableSpec ts, Page p) throws TypeException {
    StorageIterator it = p.iterator();
    while ( it.hasNext() ) {
      List<Object> fields = it.next().interpretTuple(ts.getSchema());
      String r = "";
      for (Object o : fields) { r += o.toString()+"|"; }
      System.out.println("Tuple : "+r);            
    }
  }

  void loadLineitemCsv(String tableName, String fileName) {
    Schema lineitemSchema = Lineitem.getSchema(tableName);
    TableId tId = null;
    
    // Create the table if necessary.
    if ( !dbms.hasRelation(tableName) ) {
      tId = dbms.addRelation(tableName, lineitemSchema);
      
      System.out.println(
        "Created table " + tableName +
        " with tuple size " + lineitemSchema.getTupleSize());
    } else {
      tId = dbms.getRelation(tableName);
    }
    
    System.out.println("Loading data from file " + fileName + ".");
    
    CSVLoader csv = new CSVLoader(fileName);
    csv.load(dbms, tId, lineitemSchema);
    dbms.getStorageEngine().getBufferPool().flushPages();
  }

  // Returns whether we're done processing.
  boolean processLine(String cmdStr) {
    LinkedList<String> args = parseCommand(cmdStr);
    if (args.size() < 1) return false;
    String cmd = args.pop();
    
    if (cmd.toLowerCase().equals("bye")) return true;
    else if (cmd.toLowerCase().equals("help")) {

      printHelp();      

    } else if (cmd.toLowerCase().equals("csv")) {
      
      String tableName = args.pop();
      String fileName = args.pop();

      loadLineitemCsv(tableName, fileName);

    } else if (cmd.toLowerCase().equals("load")) {

      String catalogFile = args.pop();
      
      // Reinitialize the DBMS from a catalog file.
      SlottedStorageFileFactory f = new SlottedStorageFileFactory();      
      dbms = new DbEngine<SlottedTupleId, SlottedPageHeader,
                          SlottedPage, SlottedHeapFile>(catalogFile, f);

    } else if (cmd.toLowerCase().equals("save")) {

      String catalogFile = args.pop();
      dbms.saveDatabase(catalogFile);

    } else if (cmd.toLowerCase().equals("show")) {

      System.out.println(dbms.toString());

    } else if (cmd.toLowerCase().equals("page")) {
      
      String relName = args.pop();
      TableSpec ts = null;
      try {
        ts = dbms.getStorageEngine().getCatalog().getTableByName(relName);
      } catch (NullPointerException e) {
        System.err.println("Invalid table name "+relName);
        return false;
      }

      String pageNumStr = args.pop();
      Integer pageNum = -1;
      try {
        pageNum = Integer.valueOf(pageNumStr);
      } catch (NumberFormatException e) {
        System.err.println("Invalid page number "+pageNumStr);
        return false;
      }

      Page p = getPageFromFile(ts.getId(), 0, pageNum);
      if ( p != null ) {
        try {
          printPage(ts, p);
        } catch (TypeException e) {
          System.err.println("Invalid tuple type");
          return false;
        }
      }

    } else if (cmd.toLowerCase().equals("benchmark")) {

      String benchmarkMode = args.pop();
      String tableName = args.pop();

      TableId tid = null;

      try {
          tid = dbms.getStorageEngine().getCatalog().getTableByName(tableName).getId();
      } catch (NullPointerException e) {
          System.err.println("Invalid Table Name.");
          return false;
      }

      System.out.println("Benchmarking in mode " + benchmarkMode + ".");

      try {
        boolean valid = false;
        int requests = Integer.parseInt(args.pop());
        int blocksize = Integer.parseInt(args.pop());
        if (benchmarkMode.toLowerCase().equals("sequential")) {
            
            generator.generate(tid, requests, blocksize,
                               WorkloadGenerator.Workload.Sequential, 0.0);
            valid = true;
        
        } else {
          valid = true;
          
          double p = 0.75;
          if ( args.size() > 0 ) { p = Double.parseDouble(args.pop()); }

          if (benchmarkMode.toLowerCase().equals("almost-sequential")) {
        
            generator.generate(tid, requests, blocksize,
                               WorkloadGenerator.Workload.MostlySequential, p);
        
          } else if (benchmarkMode.toLowerCase().equals("half-half")) {
          
              generator.generate(tid, requests, blocksize,
                                 WorkloadGenerator.Workload.HalfHalf, p);
          
          } else if (benchmarkMode.toLowerCase().equals("almost-random")) {
          
              generator.generate(tid, requests, blocksize,
                                 WorkloadGenerator.Workload.MostlyRandom, p);
          } else {
            valid = false;
          }
        }
        
        if ( !valid ) {
            System.out.println("Incorrect benchmark mode.");
        }
      } catch (NumberFormatException e) {
        System.err.println("Invalid integer argument for benchmark mode");
        return false;
      }
    }

    return false;
  }

  public void run() {
    BufferedReader in = new BufferedReader(new InputStreamReader(System.in)); 
    for (;;) {
      System.out.print(prompt+" ");
      try {
        String line = in.readLine();
        if (line == null) break;
        if ( processLine(line) ) break;
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static void main(String[] args) throws Exception {
      HW1Terminal terminal = new HW1Terminal("jasper>>>");
      terminal.start();
  }
}
