package edu.jhu.cs.damsl.catalog.identifiers.tuple;

import java.io.Serializable;

import org.jboss.netty.buffer.ChannelBuffer;

import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TupleId;

public class SlottedTupleId extends TupleId {
  int slotIndex;

  public SlottedTupleId(PageId pId, short length, int slotId) {
    super(pId, length);
    slotIndex = slotId;
  }
  
  public int slot() { return slotIndex; }

  @Override
  public boolean equals(Object o) {
    if ( o == null || !(o instanceof SlottedTupleId) ) { return false; }
    if ( o == this ) { return true; }
    SlottedTupleId other = (SlottedTupleId) o;
    return other.pageId.equals(pageId)
            && other.slotIndex == slotIndex && other.tupleSize == tupleSize;
  }

  @Override
  public int hashCode() { 
    return Integer.valueOf(pageId.hashCode() + slotIndex + tupleSize).hashCode();
  }

  @Override
  public String getAddressString() {
    return super.getAddressString()+":S"+slotIndex;
  }
  
  // Buffer I/O
  public void write(ChannelBuffer buf) {
    super.write(buf);
    buf.writeInt(slotIndex);
  }
  
  @Override
  public short size() { return (short) (super.size()+(Integer.SIZE>>3)); }

}
