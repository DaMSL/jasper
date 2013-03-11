package edu.jhu.cs.damsl.engine.storage.page;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.DuplicatedChannelBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TupleId;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.iterator.page.StorageIterator;
import edu.jhu.cs.damsl.factory.page.HeaderFactory;
import edu.jhu.cs.damsl.factory.tuple.TupleIdFactory;

/**
 * The basic unit of information transfer to/from disk.
 *
 * Pages are the fundamental unit of data retrieved and stored on disk during a single operation.
 * They contain a number of tuples from a given relation, the exact layout of these tuples in the
 * page depends on the type of page.
 */
public abstract class Page<IdType extends TupleId,
                           HeaderType extends PageHeader>
                        extends DuplicatedChannelBuffer
{
  
  public enum Permissions { READ, WRITE };
  
  protected static final Logger logger = LoggerFactory.getLogger(Page.class);
  
  protected PageId pageId;
  protected HeaderType header;
  
  // Create an in-memory page backed by the given buffer, to hold tuples
  // of the given schema.
  public Page(Integer id, ChannelBuffer buf, Schema sch, byte flags) {
    this(new PageId(id), buf, sch, flags);
  }

  // Create a page object representing the given page id, and buffer.
  // The header will be read from the backing buffer.
  public Page(PageId id, ChannelBuffer buf, Schema sch, byte flags) {
    super(buf);
    pageId = id;
    initializeHeader(sch, flags);
  }

  // Constructor variants  
  public Page(Integer id, ChannelBuffer buf, Schema sch) {
    this(new PageId(id), buf, sch, (byte) 0);
  }

  public Page(PageId id, ChannelBuffer buf, Schema sch) {
    this(id, buf, sch, (byte) 0);
  }

  public Page(Integer id, ChannelBuffer buf, byte flags) {
    this(new PageId(id), buf, null, flags);
  }

  public Page(PageId id, ChannelBuffer buf, byte flags) {
    this(id, buf, null, flags);
  }

  // Create a page object without initializing a new header.
  // This is a shallow copy of the page.
  public Page(Page<IdType, HeaderType> p) {
    super(p);
    pageId = p.pageId;
    header = p.header;
  }

  protected void initializeHeader(Schema sch, byte flags) {
    if ( pageId.fileId() != null ) { readHeader(); }
    else header = getHeaderFactory().getHeader(sch, this, flags);
  }

  public PageId getId() { return pageId; }
  
  public void setId(PageId id) { pageId = id; }

  // Factory accessors
  public abstract HeaderFactory<HeaderType> getHeaderFactory();

  public abstract TupleIdFactory<IdType> getTupleIdFactory();

  // Header accessors
  public HeaderType getHeader() { return header; }

  public void setHeader(HeaderType hdr) { header = hdr; }

  public void readHeader() { header = getHeaderFactory().readHeader(this); }
  
  public void writeHeader() { header.writeHeader(this); }

  public boolean isDirty() { return header.isDirty(); }
  
  public void setDirty(boolean d) { header.setDirty(d); }

  // Tuple accessors.
  
  // The default tuple retrieval method is via iteration.
  public abstract StorageIterator iterator();

  // A two-sided iterator within this page. If the first argument is null,
  // iteration starts from the beginning of the page, and if the second
  // argument is null, ends with the last tuple of the page.
  public abstract StorageIterator iterator(IdType start, IdType end);

  // Get a specific tuple from the page
  public abstract Tuple getTuple(IdType id);

  // Append a tuple to the page, supporting both fixed and variable-length tuples.
  public abstract IdType putTuple(Tuple t);

  // Insert a tuple at the exact location given by the tuple identifier.
  // The tuple type indicates the length of the give tuple, and thus whether
  // it is fixed or variable length.
  public abstract boolean insertTuple(IdType id, Tuple t);
  
  // Remove a specific tuple from this page.
  public abstract boolean removeTuple(IdType id);

  // Remove all tuples from this page.
  public abstract void removeTuples();

  // TODO
  // Defragment the page, compacting both slot and space usage.
  public void defragment() {}

}
