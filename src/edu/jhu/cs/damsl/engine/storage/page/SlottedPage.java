package edu.jhu.cs.damsl.engine.storage.page;

import org.jboss.netty.buffer.ChannelBuffer;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.iterator.page.SlottedPageIterator;
import edu.jhu.cs.damsl.engine.storage.page.factory.HeaderFactory;
import edu.jhu.cs.damsl.engine.storage.page.factory.SlottedPageHeaderFactory;
import edu.jhu.cs.damsl.utils.hw1.HW1.*;

@CS316Todo
@CS416Todo
public class SlottedPage extends Page<SlottedPageHeader> {
  
  public static final HeaderFactory<SlottedPageHeader> headerFactory 
    = new SlottedPageHeaderFactory();

  // Commonly used constructors.
  public SlottedPage(Integer id, ChannelBuffer buf) {
    super(id, buf, PageHeader.FILL_BACKWARD);
  }

  public SlottedPage(PageId id, ChannelBuffer buf) {
    super(id, buf, PageHeader.FILL_BACKWARD);
  }

  // Constructor variants
  public SlottedPage(Integer id, ChannelBuffer buf, Schema sch, byte flags) {
    super(id, buf, sch, flags);
  }

  public SlottedPage(PageId id, ChannelBuffer buf, Schema sch, byte flags) {
    super(id, buf, sch, flags);
  }

  public SlottedPage(Integer id, ChannelBuffer buf, Schema sch) {
    this(id, buf, sch, PageHeader.FILL_BACKWARD);
  }
  
  public SlottedPage(PageId id, ChannelBuffer buf, Schema sch) {
    this(id, buf, sch, PageHeader.FILL_BACKWARD);
  }

  public SlottedPage(Integer id, ChannelBuffer buf, byte flags) {
    this(id, buf, null, (byte) (flags | PageHeader.FILL_BACKWARD));
  }
  
  public SlottedPage(PageId id, ChannelBuffer buf, byte flags) {
    this(id, buf, null, (byte) (flags | PageHeader.FILL_BACKWARD));
  }

  // Construct a slotted page without initializing a header.
  public SlottedPage(SlottedPage p) { super(p); }

  // Factory accessors.
  @Override
  public HeaderFactory<SlottedPageHeader> getHeaderFactory() { 
    return headerFactory;
  }
  
  // Header accessors.
  @Override
  public SlottedPageHeader getHeader() { return header; }

  @Override
  public void setHeader(SlottedPageHeader hdr) {  header = hdr; }

  @Override
  public void readHeader() {
    header = getHeaderFactory().readHeader(this);
  }
  
  // Tuple accessors.
  @Override
  public SlottedPageIterator iterator() {
    return new SlottedPageIterator(getId(), this);
  }

  @CS316Todo
  @CS416Todo
  public Tuple getTuple(int slotIndex, int tupleSize) {
    return null;
  }
  
  @CS316Todo
  @CS416Todo
  public Tuple getTuple(int slotIndex) {
    return null;
  }

  // Adds a tuple the to start of the free space block.
  @CS316Todo
  @CS416Todo
  public boolean putTuple(Tuple t, short tupleSize) {
    return false;
  }
  
  @Override
  @CS316Todo
  @CS416Todo
  public boolean putTuple(Tuple t) {
    return false;
  }

  // Inserts a tuple at the given slot in this page, overwriting the existing
  // entry for fixed length tuples.
  @CS316Todo
  @CS416Todo
  public boolean insertTuple(Tuple t, short tupleSize, int slotIndex) {
    return false;
  }
  
  @CS316Todo
  @CS416Todo
  public boolean insertTuple(Tuple t, int slotIndex) {
    return false;
  }

  // Zeroes out the contents of the given slot.
  @CS316Todo
  @CS416Todo
  protected void clearTuple(int slotIndex) {}

  // Removes the tuple at the given slot in this page, zeroing the tuple data.
  @CS316Todo
  @CS416Todo
  public boolean removeTuple(int slotIndex) {
    return false;
  }

  @CS316Todo
  @CS416Todo
  public void clearTuples() {}

}
