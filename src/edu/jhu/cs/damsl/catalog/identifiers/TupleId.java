package edu.jhu.cs.damsl.catalog.identifiers;

import java.io.Serializable;

import org.jboss.netty.buffer.ChannelBuffer;

import edu.jhu.cs.damsl.catalog.Addressable;

public class TupleId implements Addressable, Serializable {
  PageId pageId;
  short slotIndex;

  public TupleId(PageId pId, short slotId) {
    pageId = pId;
    slotIndex = slotId;
  }
  
  public PageId getPage() { return pageId; }

  public short getSlot() { return slotIndex; }

  @Override
  public int getAddress() { return hashCode(); }

  @Override
  public String getAddressString() {
    return pageId.getAddressString()+":S"+slotIndex;
  }
  
  // Buffer I/O
  public static TupleId read(ChannelBuffer buf) {
    PageId id = PageId.read(buf);
    short idx = buf.readShort();
    return new TupleId(id, idx);
  }
  
  public void write(ChannelBuffer buf) {
    pageId.write(buf);
    buf.writeShort(slotIndex);
  }
  
  public short size() { return (short) (pageId.size()+(Short.SIZE>>3)); }

}
