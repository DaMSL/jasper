package edu.jhu.cs.damsl.catalog.identifiers.tuple;

import java.io.Serializable;

import org.jboss.netty.buffer.ChannelBuffer;

import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TupleId;

public class ContiguousTupleId extends TupleId {
  short offset;

  public ContiguousTupleId(PageId pId, short length, short offset) {
    super(pId, length);
    this.offset = offset;
  }
  
  public short offset() { return offset; }

  @Override
  public boolean equals(Object o) {
    if ( o == null || !(o instanceof ContiguousTupleId) ) { return false; }
    if ( o == this ) { return true; }
    ContiguousTupleId other = (ContiguousTupleId) o;
    return other.pageId.equals(pageId)
            && other.offset == offset && other.tupleSize == tupleSize;
  }

  @Override
  public int hashCode() { 
    return Integer.valueOf(pageId.hashCode() + offset + tupleSize).hashCode();
  }

  @Override
  public String getAddressString() {
    return super.getAddressString()+":O"+offset;
  }
  
  // Buffer I/O
  public void write(ChannelBuffer buf) {
    super.write(buf);
    buf.writeShort(offset);
  }
  
  @Override
  public short size() { return (short) (super.size()+(Short.SIZE>>3)); }

}
