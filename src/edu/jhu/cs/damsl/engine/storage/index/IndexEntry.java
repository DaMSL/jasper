package edu.jhu.cs.damsl.engine.storage.index;

import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.IndexId;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TupleId;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.factory.tuple.TupleIdFactory;

public class IndexEntry<IdType extends TupleId>
                implements Comparable<IndexEntry<IdType>>
{
  Schema schema;    // The search key schema.
  Tuple key;        // The value of the search key for this entry.
  
  PageId pageId;    // A pointer to a child page. This should only be used for internal nodes.
  IdType tupleId;   // A pointer to a tuple in a heap file. This should only be used for leaf nodes.

  // Default constructor that can be filled in by read() method.
  public IndexEntry(Schema sch) { schema = sch; }

  // Non-leaf node entry constructor
  public IndexEntry(Schema sch, Tuple k, PageId pId) {
    schema = sch;
    key = k;
    pageId = pId;
  }

  // Leaf node entry constructor
  public IndexEntry(Schema sch, Tuple k, IdType tId) {
    schema = sch;
    key = k;
    tupleId = tId;
  }

  // Accessor methods.
  public Schema schema() { return schema; }

  public Tuple key() { return key; }

  public PageId child() { return pageId; }

  public IdType tuple() { return tupleId; }

  // Comparison method.
  // For now, this performs an intepreted comparison whenever possible.
  // In the future, this should be implemented with an expression evaluation
  // over the key fields using Jasper's expression API.
  public int compareTo(IndexEntry<IdType> o) {
    int r = 0;
    if ( schema != null && schema.namedMatch(o.schema) ) {
      List<Object> keys = key.interpretTuple(schema);
      List<Object> oKeys = o.key.interpretTuple(o.schema);

      int kSize     = keys.size();
      int okSize    = oKeys.size();
      int numFields = Math.min(kSize, okSize);
      
      ListIterator<Object> kIt  = keys.listIterator();
      ListIterator<Object> okIt = oKeys.listIterator();
      while ( r == 0 && numFields > 0 && kIt.hasNext() && okIt.hasNext() ) {
        Object mine = kIt.next();
        Object others = okIt.next();
        if ( mine instanceof Integer && others instanceof Integer ) {
          r = ((Integer) mine).compareTo((Integer) others);
        }
        else if ( mine instanceof Long && others instanceof Long ) {
          r = ((Long) mine).compareTo((Long) others);
        }
        else if ( mine instanceof Float && others instanceof Float ) {
          r = ((Float) mine).compareTo((Float) others);
        }
        else if ( mine instanceof Double && others instanceof Double ) {
          r = ((Double) mine).compareTo((Double) others);
        }
        else if ( mine instanceof String && others instanceof String ) {
          r = ((String) mine).compareTo((String) others);
        }
        else throw new ClassCastException();
      }
      if ( r == 0 && kSize != okSize) { r = kSize - okSize; }
    } else {
      r = key.compareTo(o.key);
    }
    return r;
  }
  

  // Conversion methods between index entries and tuples.
  // The schema of these tuples should match the non-leaf and leaf schema below.
  
  // Populate an index entry by reading the member fields from the tuple.
  public void read(Tuple t, boolean leaf, TupleIdFactory<IdType> factory) {
    short keySize = Integer.valueOf(schema.getTupleSize() + Tuple.headerSize).shortValue();
    key = Tuple.getTuple(t.slice(0, keySize), keySize, keySize);
    if ( leaf ) {
      tupleId = factory.getTupleId(t.slice(keySize, t.capacity()));
    } else {
      pageId = PageId.read(t.slice(keySize, t.capacity()));
    }
  }

  // Create a tuple from this index entry by writing out the search key
  // and either the page or tuple id depending on whether this is a leaf
  // or non-leaf node.
  public Tuple write() {
    boolean leaf = pageId == null;
    short entrySize = getEntrySchemaSize(leaf);
    Tuple t = null;
    if ( entrySize > 0 ) {
      t = entrySize > 0 ? Tuple.emptyTuple(entrySize, false) : null;
      t.setBytes(0, key, key.getFixedLength());
      short offset = Integer.valueOf(key.getFixedLength()).shortValue();
      if ( leaf ) {
        tupleId.write(t.slice(offset, tupleId.size()));
      } else {
        pageId.write(t.slice(offset, pageId.size()));
      }
    }
    return t;
  }

  // Internal methods.
  // Hash table mapping a search key schema to the size of its
  // corresponding non-leaf index entry.
  protected static HashMap<Schema, Short> nonLeafSchemaSizes = null;

  // Hash table mapping a search key schema to the size of its
  // corresponding leaf index entry.
  protected static HashMap<Schema, Short> leafSchemaSizes = null;

  // Returns the size of the schema corresponding to the tuple
  // format for a leaf or non-leaf index entry.
  protected short getEntrySchemaSize(boolean leaf) {
    short r = -1;
    short commonSize = Integer.valueOf(schema.getTupleSize()+Tuple.headerSize).shortValue();
    Map<Schema, Short> schemaSizes = leaf? leafSchemaSizes : nonLeafSchemaSizes;
    if ( schema != null && !schemaSizes.containsKey(schema)
                        && (pageId != null || tupleId != null) )
    {
      short entrySize = Integer.valueOf(schema.getTupleSize() > 0?
        commonSize+(leaf? tupleId.size() : pageId.size()) : -1).shortValue();

      if ( entrySize > 0 ) { 
        schemaSizes.put(schema, entrySize);
        r = entrySize;
      }
    }
    return r;
  }

}
