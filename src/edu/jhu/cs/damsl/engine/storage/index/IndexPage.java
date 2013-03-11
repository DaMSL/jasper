package edu.jhu.cs.damsl.engine.storage.index;

import org.jboss.netty.buffer.ChannelBuffer;

import edu.jhu.cs.damsl.catalog.Defaults;
import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TupleId;
import edu.jhu.cs.damsl.catalog.identifiers.tuple.ContiguousTupleId;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.iterator.index.IndexEntryIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.index.IndexEntryPageIterator;
import edu.jhu.cs.damsl.engine.storage.page.ContiguousPage;
import edu.jhu.cs.damsl.factory.tuple.TupleIdFactory;
import edu.jhu.cs.damsl.utils.hw2.HW2.*;

/**
 *  A page implementation for B+ tree indexes.
 *  Index pages should contain a sequence of index entries, sorted by
 *  the index key. Lookup operations perform binary search through the key.
 */
@CS316Todo(methods = "getEntry, putEntry, removeEntry")
@CS416Todo(methods = "getEntry, putEntry, removeEntry")
public class IndexPage<IdType extends TupleId> extends ContiguousPage
{
  // Bit flag in the page header indicating whether this is a leaf node.
  public final static byte LEAF_NODE = 0x4;

  public final static double FILL_FACTOR = Defaults.defaultFillFactor;

  // A next page pointer.
  // For non-leaf pages, this is the right-most pointer for the page,
  // indicating the child page containing entries greater than the maximal
  // entry in this page.
  // For leaf pages, this is the next page in the leaf chain that can be
  // used to scan all tuples identified by the index.
  PageId nextPage;

  // Common constructors.
  public IndexPage(Integer id, ChannelBuffer buf, Schema sch, byte flags, boolean isLeaf) {
    super(id, buf, sch, (byte) (flags | (isLeaf? LEAF_NODE : 0)));
  }

  public IndexPage(PageId id, ChannelBuffer buf, Schema sch, byte flags, boolean isLeaf) {
    super(id, buf, sch, (byte) (flags | (isLeaf? LEAF_NODE : 0)));
  }

  // Factory constructors.
  // For index pages, 
  public IndexPage(Integer id, ChannelBuffer buf, Schema sch, byte flags) {
    this(id, buf, sch, flags, true);
  }

  public IndexPage(PageId id, ChannelBuffer buf, Schema sch, byte flags) {
    this(id, buf, sch, flags, true);
  }

  public IndexPage(Integer id, ChannelBuffer buf, Schema sch) {
    this(id, buf, sch, (byte) 0x0, true);
  }
  
  public IndexPage(PageId id, ChannelBuffer buf, Schema sch) {
    this(id, buf, sch, (byte) 0x0, true);
  }

  public IndexPage(Integer id, ChannelBuffer buf, byte flags) {
    this(id, buf, null, flags, true);
  }
  
  public IndexPage(PageId id, ChannelBuffer buf, byte flags) {
    this(id, buf, null, flags,  true);
  }

  public boolean isLeaf() { return getHeader().isFlagSet(LEAF_NODE); }

  public PageId getNextPage() { return nextPage; }

  /* Index page API, to access index entries.
     Index entries are converted to and from tuples with the
     IndexEntry.read() and IndexEntry.write() methods. The corresponding
     tuples can then be written to and read from the ContiguousPage from
     which this IndexPage inherits.
  */

  // Retrieves the smallest index entry greater than the given key.
  // This should perform a binary search through the index entries on the page.
  @CS316Todo(exercise = 1)
  @CS416Todo(exercise = 1)
  public IndexEntry<IdType> getEntry(Tuple key) {
    throw new UnsupportedOperationException();
  }

  // Adds the given entry in sorted fashion to this index page.
  @CS316Todo(exercise = 2)
  @CS416Todo(exercise = 2)
  public boolean putEntry(IndexEntry<IdType> entry) {
    throw new UnsupportedOperationException();
  }

  // Removes the given from this index page.
  @CS316Todo(exercise = 3)
  @CS416Todo(exercise = 3)
  public boolean removeEntry(IndexEntry<IdType> entry) {
    throw new UnsupportedOperationException();
  }

  // An iterator over the entries in this page.
  public IndexEntryIterator<IdType> entry_iterator(
            Schema key, TupleIdFactory<IdType> factory)
  {
    return new IndexEntryPageIterator<IdType>(
                  getId(), this, key, factory);
  }

  public IndexEntryIterator<IdType> entry_iterator(
            Schema key, TupleIdFactory<IdType> factory,
            ContiguousTupleId start, ContiguousTupleId end)
  {
    return new IndexEntryPageIterator<IdType>(
                  getId(), this, key, factory, start, end);
  }

}
