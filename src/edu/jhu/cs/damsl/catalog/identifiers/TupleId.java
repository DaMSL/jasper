package edu.jhu.cs.damsl.catalog.identifiers;

import java.io.Serializable;

import org.jboss.netty.buffer.ChannelBuffer;

import edu.jhu.cs.damsl.catalog.Addressable;

public abstract class TupleId implements Addressable, Serializable {
  
  // Page storing the tuple identified by this object.
  protected PageId pageId; 

  // Length of the tuple identified by this object, -1 for variable length.
  protected short tupleSize;

  public TupleId(PageId pId, short length) {
    pageId = pId;
    tupleSize = length;
  }
  
  public PageId pageId() { return pageId; }
  public short tupleSize() { return tupleSize; }

  @Override
  public int getAddress() { return hashCode(); }

  @Override
  public String getAddressString() {
    return pageId.getAddressString()+":L"+Integer.toString(tupleSize);
  }
  
  // Buffer I/O
  // See TupleIdFactory and its inherited classes for constructing and
  // reading a tuple from a ChannelBuffer.

  public void write(ChannelBuffer buf) {
    pageId.write(buf);
    buf.writeShort(tupleSize);
  }
  
  public short size() { return (short) (pageId.size()+(Short.SIZE>>3)); }

}
