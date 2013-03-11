package edu.jhu.cs.damsl.utils.hw2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.IndexId;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TableId;
import edu.jhu.cs.damsl.catalog.identifiers.TupleId;
import edu.jhu.cs.damsl.catalog.identifiers.tuple.SlottedTupleId;
import edu.jhu.cs.damsl.catalog.specs.TableSpec;
import edu.jhu.cs.damsl.engine.dbms.DbEngine;
import edu.jhu.cs.damsl.engine.dbms.DbEngine;
import edu.jhu.cs.damsl.engine.storage.file.SlottedHeapFile;
import edu.jhu.cs.damsl.engine.storage.iterator.page.StorageIterator;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPage;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPageHeader;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.transactions.TransactionAbortException;
import edu.jhu.cs.damsl.factory.file.SlottedStorageFileFactory;
import edu.jhu.cs.damsl.language.core.types.*;
import edu.jhu.cs.damsl.utils.CSVLoader;
import edu.jhu.cs.damsl.utils.Lineitem;
import edu.jhu.cs.damsl.utils.WorkloadGenerator;
import edu.jhu.cs.damsl.utils.hw1.HW1Terminal;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

import static edu.jhu.cs.damsl.utils.hw2.HW2Terminal.LineStatus.*;

public class HW2Terminal {

    public enum LineStatus {
        LINE_UNKNOWN,
        LINE_CONTINUE,
        LINE_TERMINATE
    }

    String prompt;

    DbEngine<SlottedTupleId, SlottedPageHeader, SlottedPage, SlottedHeapFile> dbms;
    WorkloadGenerator<SlottedTupleId, SlottedPageHeader, SlottedPage, SlottedHeapFile> generator;

    public HW2Terminal(String prompt) {
        this.prompt = prompt;
        SlottedStorageFileFactory f = new SlottedStorageFileFactory();
        dbms = new DbEngine<SlottedTupleId, SlottedPageHeader, SlottedPage, SlottedHeapFile>(f);

        generator = 
            new WorkloadGenerator<SlottedTupleId, SlottedPageHeader, SlottedPage, SlottedHeapFile>(dbms);
    }

    public void printHelp() {

        String[] cmds = new String[] {
            "--- Available Commands ---",
            "help                       -- Print this list.",
            "create <relation>          -- Create an empty relation.",
            "load   <relation> <file>   -- Load the contents of a file into a relation.",
            "index  <relation> <keys>   -- Create a new index on an existing relation.",
            "indexes <relation>         -- Show the indexes for a given relation.",
            "insert <relation> <values> -- Insert a tuple into a relation.",
            "lookup <relation> <values> -- Look up a set of values in a relation.",
            "delete <relation> <values> -- Delete a tuple from a relation.",
            "page   <relation> <page>   -- Show the contents of a given page from a relation.",
            "bye                        -- Leave."
        };

        String help = ""; for (String c : cmds) { help += c+"\n"; }
        System.out.print(help);
    }

    protected LineStatus processLine(String cmdStr) {

        LinkedList<String> args = new LinkedList<String>(Arrays.asList(cmdStr.split(" ")));

        if (args.size() < 1) {
            return LINE_CONTINUE;
        }

        String cmd = args.pop().toLowerCase();

        if (cmd.equals("help")) {
            printHelp();
            return LINE_CONTINUE;
        } else if (cmd.equals("create")) {
            if (args.size() < 1) {
                System.out.println("usage: create <relation>");
                return LINE_CONTINUE;
            }

            String tableName = args.pop();
            Schema liSchema = Lineitem.getSchema(tableName);
            TableId tid = null;

            // Create the table if necessary.
            if (!dbms.hasRelation(tableName)) {
                tid = dbms.addRelation(tableName, liSchema);

                System.out.println(
                    "Created table " + tableName + " with tuple size " + liSchema.getTupleSize()
                );
            } else {
                System.out.println("Table '" + tableName + "' already exists.");
            }

            return LINE_CONTINUE;

        } else if (cmd.equals("load")) {
            if (args.size() != 2) {
                System.out.println("usage: load <relation> <filepath>");
                return LINE_CONTINUE;
            }

            String tableName = args.pop();
            String fileName = args.pop();

            if (!dbms.hasRelation(tableName)) {
                System.out.println("Relation '" + tableName + "' does not exist.");
                return LINE_CONTINUE;
            }

            TableId tId = dbms.getRelation(tableName);

            CSVLoader csv = new CSVLoader(fileName);
            csv.load(dbms, tId, Lineitem.getSchema(tableName));
            dbms.getStorageEngine().getBufferPool().flushPages();

            return LINE_CONTINUE;
        } else if (cmd.equals("index")) {
            if (args.size() != 2) {
                System.out.println("usage: index <relation> <keys>");
                return LINE_CONTINUE;
            }

            String rel = args.pop();
            String keyString = args.pop();
            List<String> values = new LinkedList<String>(Arrays.asList(keyString.split(",")));

            TableSpec ts = dbms.getStorageEngine().getCatalog().getTableByName(rel);

            Schema schema = null;
            try {
                schema = ts.getSchema();
            } catch (TypeException e) {
                System.err.println("Unable to retrieve schema for relation " + rel + ".");
                return LINE_CONTINUE;
            }

            LinkedHashMap<String, Type> fts = new LinkedHashMap<String, Type>();

            Iterator<String> vi = values.iterator();
            Iterator<Type> ti = schema.getTypes().iterator();
            Iterator<String> fi = schema.getFields().iterator();

            while (vi.hasNext() && ti.hasNext() && fi.hasNext()) {
                String s = vi.next();
                Type t = ti.next();
                String f = fi.next();
                if (s.equals("")) {
                    continue;
                }
                fts.put(f, t);
            }

            Schema keySchema = new Schema(rel + keyString, fts);

            IndexId iid = dbms.getStorageEngine().getCatalog().addIndex(ts.getId(), keySchema);

            System.out.println("Added index " + iid.getAddressString() + " for relation " + rel + ".");

            return LINE_CONTINUE;
        } else if (cmd.equals("indexes")) {
            if (args.size() != 1) {
                System.out.println("usage: indices <relation>");
                return LINE_CONTINUE;
            }

            String rel = args.pop();

            TableSpec ts = dbms.getStorageEngine().getCatalog().getTableByName(rel);
            LinkedList<IndexId> indexes = dbms.getStorageEngine().getCatalog().getIndexes(ts.getId());

            for (IndexId iid : indexes) {
                System.out.println("Found " + iid.getAddressString() + ".");
            }

            return LINE_CONTINUE;
        } else if (cmd.equals("insert")) {
            if (args.size() != 2) {
                System.out.println("usage: insert <relation> <values>");
                return LINE_CONTINUE;
            }

            String rel = args.pop();
            List<String> values = new LinkedList<String>(Arrays.asList(args.pop().split(",")));

            TableSpec ts = dbms.getStorageEngine().getCatalog().getTableByName(rel);

            Schema schema = null;
            try {
                schema = ts.getSchema();
            } catch (TypeException e) {
                System.err.println("Unable to retrieve schema for relation " + rel + ".");
                return LINE_CONTINUE;
            }

            List<Object> fields = new LinkedList<Object>();

            Iterator<String> vi = values.iterator();
            Iterator<Type> ti = schema.getTypes().iterator();

            while (vi.hasNext() && ti.hasNext()) {
                String s = vi.next();
                Type t = ti.next();
                fields.add(t.parseType(s));
            }

            Tuple t = Tuple.schemaTuple(schema, fields);

            try {
                dbms.getStorageEngine().insertTuple(null, ts.getId(), t);
            } catch (TransactionAbortException e) {
                System.err.println("Transaction Aborted.");
            }

            return LINE_CONTINUE;
        } else if (cmd.equals("lookup")) {
            if (args.size() != 2) {
                System.out.println("usage: lookup <relation> <values>");
                return LINE_CONTINUE;
            }

            String rel = args.pop();
            List<String> values = new LinkedList<String>(Arrays.asList(args.pop().split(",")));

            TableSpec ts = dbms.getStorageEngine().getCatalog().getTableByName(rel);

            Schema schema = null;
            try {
                schema = ts.getSchema();
            } catch (TypeException e) {
                System.err.println("Unable to retrieve schema for relation " + rel + ".");
                return LINE_CONTINUE;
            }

            List<Object> fields = new LinkedList<Object>();

            Iterator<String> vi = values.iterator();
            Iterator<Type> ti = schema.getTypes().iterator();
            Iterator<String> fi = schema.getFields().iterator();

            LinkedHashMap<String, Type> fts = new LinkedHashMap<String, Type>();

            while (vi.hasNext() && ti.hasNext() && fi.hasNext()) {
                String s = vi.next();
                Type t = ti.next();
                String f = fi.next();
                if (s.equals("")) {
                    continue;
                }
                fields.add(t.parseType(s));
                fts.put(f, t);
            }

            Schema keySchema = new Schema("", fts);
            Tuple t = Tuple.schemaTuple(keySchema, fields);

            IndexId matching_index = null;

            for (IndexId iid : dbms.getStorageEngine().getCatalog().getIndexes(ts.getId())) {
                if (iid.schema().namedMatch(keySchema)) {
                    matching_index = iid;
                    break;
                }
            }


            if (matching_index != null) {
                Iterator<SlottedTupleId> tis = null;
                try {
                    tis = dbms.getStorageEngine().seekIndex(null, matching_index, t, null);
                } catch (TransactionAbortException e) {
                    e.printStackTrace();
                    return LINE_CONTINUE;
                }

                for (SlottedTupleId tid; tis.hasNext(); ) {
                    tid = tis.next();
                    SlottedPage p = dbms.getStorageEngine().getPage(null, tid.pageId(), null);
                    Tuple match = p.getTuple(tid);
                    System.out.println(match.toString(schema));
                }

            } else {
                StorageIterator tis = dbms.getStorageEngine().scanRelation(null, ts.getId(), null);
                for (Tuple tid; tis.hasNext(); ) {
                    tid = tis.next();
                    if (tid.compareTo(t) == 0) {
                        System.out.println(tid.toString(schema));
                    }
                }
            }

            return LINE_CONTINUE;
        } else if (cmd.equals("delete")) {
            if (args.size() != 2) {
                System.out.println("usage: index <relation> <values>");
                return LINE_CONTINUE;
            }

            String rel = args.pop();
            List<String> values = new LinkedList<String>(Arrays.asList(args.pop().split(",")));

            TableSpec ts = dbms.getStorageEngine().getCatalog().getTableByName(rel);

            Schema schema = null;
            try {
                schema = ts.getSchema();
            } catch (TypeException e) {
                System.err.println("Unable to retrieve schema for relation " + rel + ".");
                return LINE_CONTINUE;
            }

            List<Object> fields = new LinkedList<Object>();

            Iterator<String> vi = values.iterator();
            Iterator<Type> ti = schema.getTypes().iterator();
            Iterator<String> fi = schema.getFields().iterator();

            LinkedHashMap<String, Type> fts = new LinkedHashMap<String, Type>();

            while (vi.hasNext() && ti.hasNext() && fi.hasNext()) {
                String s = vi.next();
                Type t = ti.next();
                String f = fi.next();
                if (s.equals("")) {
                    continue;
                }
                fields.add(t.parseType(s));
                fts.put(f, t);
            }

            Schema keySchema = new Schema("", fts);
            Tuple t = Tuple.schemaTuple(keySchema, fields);

            IndexId matching_index = null;

            for (IndexId iid : dbms.getStorageEngine().getCatalog().getIndexes(ts.getId())) {
                if (iid.schema().namedMatch(keySchema)) {
                    matching_index = iid;
                    break;
                }
            }

            if (matching_index != null) {
                Iterator<SlottedTupleId> tis = null;
                try {
                    tis = dbms.getStorageEngine().seekIndex(null, matching_index, t, null);
                } catch (TransactionAbortException e) {
                    e.printStackTrace();
                    return LINE_CONTINUE;
                }

                LinkedList<SlottedTupleId> tids = new LinkedList<SlottedTupleId>();

                // Copy tids to avoid concurrent modification issues.
                for (SlottedTupleId tid; tis.hasNext(); ) {
                    tid = tis.next();
                    tids.add(tid);
                }

                for (SlottedTupleId tid : tids) {
                    try {
                        dbms.getStorageEngine().deleteTuple(null, ts.getId(), tid);
                    } catch (TransactionAbortException e) {
                        e.printStackTrace();
                        return LINE_CONTINUE;
                    }
                }

            } else {
                StorageIterator tis = dbms.getStorageEngine().scanRelation(null, ts.getId(), null);
                for (Tuple tid; tis.hasNext(); ) {
                    tid = tis.next();
                    if (tid.compareTo(t) == 0) {
                        tis.remove();
                    }
                }
            }

            return LINE_CONTINUE;
        } else if (cmd.equals("page")) {
            if (args.size() != 2) {
                System.out.println("usage: page <relation> <page>");
                return LINE_CONTINUE;
            }

            String rel = args.pop();
            TableSpec ts = null;

            try {
                ts = dbms.getStorageEngine().getCatalog().getTableByName(rel);
            } catch (NullPointerException e) {
                System.err.println("Table " + rel + " does not exist.");
                return LINE_CONTINUE;
            }

            Integer pageNum = null;

            try {
                pageNum = Integer.valueOf(args.pop());
            } catch (NumberFormatException e) {
                System.err.println("Invalid Page Number.");
                return LINE_CONTINUE;
            }

            Page p = getPageFromFile(ts.getId(), 0, pageNum);

            if ( p != null ) {
                try {
                    printPage(ts, p);
                } catch (TypeException e) {
                    System.err.println("Invalid tuple type");
                    return LINE_CONTINUE;
                }
            }

            return LINE_CONTINUE;
        } else if (cmd.equals("bye")) {
            return LINE_TERMINATE;
        } else {
            return LINE_UNKNOWN;
        }
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

    public void loop() {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            System.out.print(prompt + " ");
            try {
                String line = in.readLine();
                if (line == null)
                    break;
                if (processLine(line) == LINE_TERMINATE)
                    break;
            } catch (IOException e) {
                e.printStackTrace();
            } catch (UnsupportedOperationException e) {
                e.printStackTrace();
                System.out.println("A necessary method has not been implemented.");
            }
        }

        return;
    }

    public static void main(String[] args) throws Exception {
        HW2Terminal terminal = new HW2Terminal("jasper-hw2>>>");
        terminal.loop();
        return;
    }

}
