package edu.jhu.cs.damsl.engine.storage.page;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;

import org.jboss.netty.buffer.ChannelBuffer;

import edu.jhu.cs.damsl.catalog.Defaults;
import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.utils.hw1.HW1.*;

@CS316Todo
@CS416Todo
public class SlottedPageHeader extends PageHeader {

  public static final short SLOT_SIZE = (Short.SIZE>>3)*2;
  public static final short INVALID_SLOT = -1;

  public class Slot {
    public short offset;
    public short length;
    
    public Slot() { offset = -1; length = -1; }
    public Slot(short off, short len) { offset = off; length = len; }
  }

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
    this(flags, tupleSize, (short) buf.capacity());
  }

  public SlottedPageHeader(byte flags, short tupleSize, short bufCapacity)
  {
    super(flags, tupleSize, bufCapacity);
    
    short numSlots = 
      (tupleSize <= 0? Integer.valueOf(-1).shortValue() : 
        Integer.valueOf(
          (bufCapacity - (super.getHeaderSize()+((Short.SIZE>>3)*2)))
              / (tupleSize + SLOT_SIZE)).shortValue());
    
    resetHeader(numSlots, (short) 0);
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
  
  /**
    * Resets this header, altering the number of slots maintained in
    * the page directory, and setting the free slot search marker to
    * the given slot index.
    */
  @CS316Todo
  @CS416Todo
  public void resetHeader(short numSlots, short slotIdx) {}
  
  /**
    * Resets the header, clearing the current state of the slot directory,
    * and setting the free slot search marker to the first slot.
    */
  @CS316Todo
  @CS416Todo
  public void resetHeader() {}
  
  /**
   * Writes this header to the buffer backing this page.
   */
  @CS316Todo
  @CS416Todo
  @Override
  public void writeHeader(ChannelBuffer buf) {}

  /**
   * Returns the size of the header, in bytes.
   */
  @CS316Todo
  @CS416Todo
  @Override
  public short getHeaderSize() { return -1; }


  /**
   * Returns whether the page associated with this header has the
   * given amount of space available for use.
   */  
  @CS316Todo
  @CS416Todo
  @Override
  public boolean isSpaceAvailable(short size) { return false; }

  /**
    * Returns the number of slots in the header.
    */
  @CS316Todo
  @CS416Todo
  public int getNumSlots() { return -1; }
  
  /**
    * Returns the slot at the given index in the slot directory.
    */
  @CS316Todo
  @CS416Todo
  public Slot getSlot(int index) { return null; }

  /**
    * Expands the slot directory to ensure that it can contain
    * the given slot at the specified index. Blank slots should
    * be added to the slot directory as needed.
    */
  @CS316Todo
  @CS416Todo
  public void growSlots(int index, Slot s) {}

  /**
    * Sets the slot at the given index.
    */
  @CS316Todo
  @CS416Todo
  public void setSlot(int index, Slot s) throws IndexOutOfBoundsException {}

  /**
    * Sets the slot at the given index to point to the given offset
    * and length within the page.
    */
  @CS316Todo
  @CS416Todo
  public void setSlot(int index, short offset, short length) {}

  /**
    * Returns the page offset of the i'th slot.
    */
  @CS316Todo
  @CS416Todo
  public short getSlotOffset(int slot) { return -1; }
  
  /**
    * Returns the tuple size of the i'th slot.
    */
  @CS316Todo
  @CS416Todo
  public short getSlotLength(int slot) { return -1; }
  
  @CS316Todo
  @CS416Todo
  public short getRequiredSpace(int slotIndex, short reqSpace) { return -1; }

  /**
    * Returns the index of the next free slot.
    */
  @CS316Todo
  @CS416Todo
  public int getNextSlot() { return -1; }

  /**
    * Advances the free slot index.
    */
  @CS316Todo
  @CS416Todo  
  void advanceSlot() {}

  /**
    * Indicates whether this header can resize its slot directory.
    */
  @CS316Todo
  @CS416Todo
  public boolean hasDynamicSlots() { return false; }

  /**
    * Indicates whether this slot at the given index exists.
    */
  @CS316Todo
  @CS416Todo
  public boolean isValidSlot(int index) { return false; }

  /**
    * Returns whether the requested index contains a valid tuple (i.e.
    * a non-negative offset and length).
    */
  @CS316Todo
  @CS416Todo
  public boolean isValidTuple(int index) { return false; }

  /**
    * Returns whether a tuple of the given size can be written at
    * the given slot index.
    */
  @CS316Todo
  @CS416Todo
  public boolean isValidPut(int slotIndex, short size) { return false; }
 
  /**
    * Returns whether a tuple of the given size can be written to the
    * underlying page.
    */
  @CS316Todo
  @CS416Todo
  public boolean isValidAppend(short size) { return false; }

  /**
    * Uses the slot at the given index, and returns whether the operation
    * succeeded.
    */
  @CS316Todo
  @CS416Todo
  public boolean useSlot(int slotIndex, short tupleSize) { return false; }

  /**
    * Uses the next free slot, and returns whether the operation succeeded.
    */
  @CS316Todo
  @CS416Todo
  public int useNextSlot(short tupleSize) { return -1; }

  /**
    * Resets a specific slot.
    */
  @CS316Todo
  @CS416Todo
  public void resetSlot(int slotIndex) {}

  /**
    * Returns a human readable representation of this header;
    */
  @CS316Todo
  @CS416Todo
  public String toString() { return ""; }

}
