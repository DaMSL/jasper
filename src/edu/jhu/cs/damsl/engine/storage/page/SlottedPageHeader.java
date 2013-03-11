package edu.jhu.cs.damsl.engine.storage.page;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;

import org.jboss.netty.buffer.ChannelBuffer;

import edu.jhu.cs.damsl.catalog.Defaults;
import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.engine.storage.Tuple;

public class SlottedPageHeader extends PageHeader {

  // Each tuple slot can be at most 32Kb large
  public class Slot {
    public short offset;
    public short length;
    
    public Slot() { offset = -1; length = -1; }
    public Slot(short off, short len) { offset = off; length = len; }
  }
  
  public static final short SLOT_SIZE = (Short.SIZE>>3)*2;
  public static final short INVALID_SLOT = -1;

  short slotCapacity, slotIndex;
  ArrayList<Slot> slots;

  public SlottedPageHeader(ChannelBuffer buf) {
    this((byte) 0x0, (short) -1, buf);
  }

  public SlottedPageHeader(Schema sch, ChannelBuffer buf, byte flags) {
    this(flags,
         (short) (sch == null? -1 : (sch.getTupleSize()+Tuple.headerSize)),
         buf);
  }

  public SlottedPageHeader(byte flags, short tupleSize, ChannelBuffer buf)
  {
    this(flags, tupleSize, (short) buf.capacity(),
         (short) (tupleSize <= 0? -1 : buf.capacity() / tupleSize), (short) 0);
  }

  public SlottedPageHeader(byte flags, short tupleSz, short bufCapacity,
                           short numSlots, short slotIdx)
  {
    super(flags, tupleSz, bufCapacity);
    resetHeader(numSlots, slotIdx);
  }
  
  public SlottedPageHeader(PageHeader h, short numSlots, short slotIdx) {
    super(h);
    resetHeader(numSlots, slotIdx);
  }
  
  public void resetHeader(short numSlots, short slotIdx) {
    slotCapacity = numSlots;
    slotIndex = (short) (slotCapacity > 0? slotIdx : -1);
    slots = new ArrayList<Slot>(
        slotCapacity < 0 ? Defaults.defaultPageSlots : slotCapacity);
    headerSize = getHeaderSize();

    if ( slotCapacity > 0 ) {
      for (int i = 0; i < slotCapacity; ++i) { slots.add(new Slot()); }
    }
    super.resetHeader();
  }
  
  public void resetHeader() { resetHeader(slotCapacity, (short) 0); }
  
  // Write the in-memory header to the buffer backing this page.
  @Override
  public void writeHeader(ChannelBuffer buf) {
    super.writeHeader(buf);
    buf.writeShort(slotCapacity);
    if ( slotCapacity > 0 ) buf.writeShort(slotIndex);
    if ( slotCapacity <= 0 ) buf.writeShort(slots.size());
    for ( Slot s : slots ) {
      buf.writeShort(s.offset);
      buf.writeShort(s.length);
    }
  }

  // Returns the size of the disk-based header repr, in bytes.
  @Override
  public short getHeaderSize() {
    return Integer.valueOf(
          super.getHeaderSize()+
          (Short.SIZE>>3)*2+(getNumSlots()>0? getNumSlots()*SLOT_SIZE : 0)
        ).shortValue();
  }

  @Override
  public boolean isSpaceAvailable(short size) {
    return isValidAppend(size);
  }

  // Number of slots in the header.
  public int getNumSlots() {
    return slotCapacity > 0? slotCapacity : (slots == null ? 0 : slots.size());
  }
  
  public Slot getSlot(int index) {
    Slot r = null;
    try {
      r = slots.get(index);
    } catch (IndexOutOfBoundsException e) {}
    return r;
  }

  public void growSlots(int index, Slot s) {
    slots.ensureCapacity(index+1);
    for (int i = getNumSlots(); i < index; ++i) {
      slots.add(i, new Slot());
      headerSize += SLOT_SIZE;
    }
    slots.add(index, s);
    headerSize += SLOT_SIZE;
  }

  public void setSlot(int index, Slot s) throws IndexOutOfBoundsException {
    if ( hasDynamicSlots() && index >= getNumSlots() ) { growSlots(index, s); }
    else if ( isValidSlot(index) ) { slots.set(index, s); }
    else throw new IndexOutOfBoundsException();
  }

  public void setSlot(int index, short offset, short length) {
    setSlot(index, new Slot(offset, length));
  }

  // Page offset and length of the i'th slot.
  public short getSlotOffset(int slot) {
    Slot s = getSlot(slot);
    return ( s == null? -1 : s.offset );
  }
  
  public short getSlotLength(int slot) {
    Slot s = getSlot(slot);
    return ( s == null? -1 : s.length );
  }
  
  public short getRequiredSpace(int slotIndex, short reqSpace) {
    if ( hasDynamicSlots() ) {
      // Compute space needed for both dynamic slots and the tuple.
      int requiredSlots =
        slotIndex >= slots.size()? (slotIndex+1)-slots.size() : slots.size();
      return (short) ((requiredSlots > 0 ? requiredSlots : 0)*SLOT_SIZE+reqSpace);
    }
    return reqSpace;
  }

  public int getNextSlot() {
    int r = -1;
    if ( hasDynamicSlots() ) { r = slots.size(); }
    else { r = isValidSlot(slotIndex)? slotIndex : -1; }
    return r;
  }
  
  void advanceSlot() {
    if ( !hasDynamicSlots() && slotIndex >= 0) { slotIndex++; }
  }

  public boolean hasDynamicSlots() { return slotCapacity <= 0; }

  public boolean isValidSlot(int index) {
    return index >= 0 && index < getNumSlots();
  }

  // Returns whether the requested index contains a valid tuple (i.e.
  // a non-negative offset and length).
  public boolean isValidTuple(int index) {
    boolean valid = isValidSlot(index);
    Slot s = valid ? getSlot(index) : null;
    return ( valid && s.offset >= headerSize && s.length > 0 );
  }

  public boolean isValidPut(int slotIndex, short size) {
    return isValidTupleSize(size)
        && isValidSlot(slotIndex)
        && getFreeSpace() > getRequiredSpace(slotIndex, size);
  }
  
  public boolean isValidAppend(short size) {
    return isValidTupleSize(size)
        && getFreeSpace() > getRequiredSpace(getNextSlot(), size);
  }

  public boolean useSlot(int slotIndex, short tupleSize) {
    boolean r = false;
    if ( r = isValidPut(slotIndex, tupleSize) ) {
      if ( filledBackward() ) useSpace(tupleSize);
      setSlot(slotIndex, freeSpaceOffset, tupleSize);
      if ( !filledBackward() ) useSpace(tupleSize);
    }
    return r;
  }

  public int useNextSlot(short tupleSize) {
    int r = -1;
    if ( isValidAppend(tupleSize) ) {
      r = getNextSlot();
      if ( filledBackward() ) useSpace(tupleSize);
      setSlot(r, freeSpaceOffset, tupleSize);
      advanceSlot();
      if ( !filledBackward() ) useSpace(tupleSize);
    }
    return r;
  }

  public void resetSlot(int slotIndex) {
    setSlot(slotIndex, new Slot());
  }

  public String toString() {
    String r = super.toString() + " slots:[";
    for (int i = 0; i < slots.size(); ++i) {
      Slot s = slots.get(i);
      if ( s.length > 0 ) {
        r += Integer.toString(i) + ":" + s.offset + "," + s.length + ";";
      }
    }
    r += "]";
    return r;
  }

}
