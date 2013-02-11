package edu.jhu.cs.damsl.engine.storage.page;

import org.jboss.netty.buffer.ChannelBuffer;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.iterator.page.ContiguousPageIterator;
import edu.jhu.cs.damsl.engine.storage.page.factory.PageFactory;
import edu.jhu.cs.damsl.engine.storage.page.factory.HeaderFactory;
import edu.jhu.cs.damsl.engine.storage.page.factory.PageHeaderFactory;
import edu.jhu.cs.damsl.utils.hw1.HW1.*;

@CS316Todo
@CS416Todo
public class ContiguousPage extends Page<PageHeader> {

  public static final HeaderFactory<PageHeader> headerFactory 
    = new PageHeaderFactory();

  // Constructor variants.
  public ContiguousPage(Integer id, ChannelBuffer buf, Schema sch, byte flags) {
    super(id, buf, sch, flags);
  }

  public ContiguousPage(PageId id, ChannelBuffer buf, Schema sch, byte flags) {
    super(id, buf, sch, flags);
  }

  public ContiguousPage(Integer id, ChannelBuffer buf, Schema sch) {
    this(id, buf, sch, (byte) 0);
  }
  
  public ContiguousPage(PageId id, ChannelBuffer buf, Schema sch) {
    this(id, buf, sch, (byte) 0);
  }

  public ContiguousPage(Integer id, ChannelBuffer buf, byte flags) {
    this(id, buf, null, flags);
  }
  
  public ContiguousPage(PageId id, ChannelBuffer buf, byte flags) {
    this(id, buf, null, flags);
  }


  // Construct a contiguous page without initializing a header.
  public ContiguousPage(ContiguousPage p) throws InvalidPageException
  {
    super(p);
    if ( header.getTupleSize() <= 0 ) throw new InvalidPageException();
  }

  // Factory accessors
  @Override
  public HeaderFactory<PageHeader> getHeaderFactory() {
    return headerFactory;
  }

  @Override
  public ContiguousPageIterator iterator() {
    return new ContiguousPageIterator(getId(), this);
  }
  
  @CS416Todo
  protected boolean validTupleBoundary(short offset) { return false; }
  
  @CS416Todo
  public boolean isValidData(short offset, short length) { return false; }
  
  @CS416Todo
  public boolean isValidOffset(short offset) { return false; }

  @CS416Todo
  protected ChannelBuffer getBuffer(short offset, short length) {
    return null;
  }

  @CS416Todo
  public Tuple getTuple(short offset, short length) {
    return null;
  }

  @CS416Todo
  public Tuple getTuple(short offset) {
    return null;
  }
  
  @CS416Todo
  protected boolean putBuffer(ChannelBuffer buf, short length) {
    return false;
  }

  @CS416Todo
  @Override
  public boolean putTuple(Tuple t, short tupleSize) {
    return false;
  }

  @CS416Todo
  @Override
  public boolean putTuple(Tuple t) {
    return false;
  }

  @CS416Todo
  protected void shiftBuffer(short offset, short shift) {}
  
  // Add a buffer at the given offset, shifting existing data.
  @CS416Todo
  protected boolean insertBuffer(short offset, ChannelBuffer buf, short length) {
    return false;
  }
  
  @CS416Todo
  public boolean insertTuple(short offset, Tuple t, short tupleSize) {
    return false;
  }
  
  @CS416Todo
  public boolean insertTuple(short offset, Tuple t) {
    return false;
  }

  @CS416Todo
  protected void removeBuffer(short offset, short removeLength, short dataLength) {}

  @CS416Todo
  public boolean removeTuple(short offset, short length) {
    return false;
  }

  @CS416Todo
  public boolean removeTuple(short offset) {
    return false;
  }

  @CS416Todo
  public void clearTuples() {}

}
