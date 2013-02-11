package edu.jhu.cs.damsl.engine.storage.page;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.DuplicatedChannelBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.iterator.page.StorageIterator;
import edu.jhu.cs.damsl.engine.storage.page.factory.HeaderFactory;

/**
 * The basic unit of information transfer to/from disk.
 *
 * Pages are the fundamental unit of data retrieved and stored on disk during a single operation.
 * They contain a number of tuples from a given relation, the exact layout of these tuples in the
 * page depends on the type of page.
 */
public abstract class Page<HeaderType extends PageHeader>
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
  public Page(Page<HeaderType> p) {
    super(p);
    pageId = p.pageId;
    header = p.header;
  }

  public PageId getId() { return pageId; }
  
  public void setId(PageId id) { pageId = id; }

  // Factory accessors
  public abstract HeaderFactory<HeaderType> getHeaderFactory();

  // Header accessors
  protected void initializeHeader(Schema sch, byte flags) {
    if ( pageId.fileId() != null ) { readHeader(); }
    else header = getHeaderFactory().getHeader(sch, this, flags);
  }

  public HeaderType getHeader() { return header; }

  public void setHeader(HeaderType hdr) { header = hdr; }

  public void readHeader() { header = getHeaderFactory().readHeader(this); }
  
  public void writeHeader() { header.writeHeader(this); }

  public boolean isDirty() { return header.isDirty(); }
  
  public void setDirty(boolean d) { header.setDirty(d); }

  // Tuple accessors.
  
  // The default tuple retrieval method is via iteration.
  public abstract StorageIterator iterator();

  // Append a variable-length tuple to the page.
  public abstract boolean putTuple(Tuple t, short requestedSize);

  // Append a fixed-size tuple to the page.
  public abstract boolean putTuple(Tuple t);

}
