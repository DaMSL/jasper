package edu.jhu.cs.damsl.engine.storage.page;

import org.jboss.netty.buffer.ChannelBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.engine.storage.Tuple;

// By default, a page is filled backwards, as for slotted pages.

public class PageHeader {

  protected static final Logger logger = LoggerFactory.getLogger(PageHeader.class);

  public static final byte DIRTY = (byte) 0x80;
  public static final byte FILL_BACKWARD = (byte) 0x40;
  
  public static final short VARIABLE_LENGTH = -1;
  public static final short INVALID_OFFSET = -1;

  public enum FillDirection { FORWARD, BACKWARD };

  byte flags;

  short tupleSize, headerSize;

  // Free space offset is stored as an absolute value, from the beginning of
  // the page, both with the in-mem and disk representation.
  // The amount of free space is thus capacity - freeSpaceOffset.
  short freeSpaceOffset, bufCapacity;
  
  // Construct a header for an in-memory page.
  public PageHeader(ChannelBuffer buf) {
    this((byte) 0x0, VARIABLE_LENGTH, (short) buf.capacity());
  }
  
  // Construct a header for a page with the given schema.
  public PageHeader(Schema sch, ChannelBuffer buf, byte flags) {
    this(flags,
        (sch == null? VARIABLE_LENGTH
                      : (short) (sch.getTupleSize()+Tuple.headerSize)),
        (short) buf.capacity());
  }
  
  // Header factory constructors.
  public PageHeader(byte flags, short tupleSz, short capacity) {
    this.flags = flags;
    tupleSize = tupleSz;
    headerSize = getHeaderSize();
    bufCapacity = capacity;
    freeSpaceOffset = filledBackward()? capacity : headerSize;
  }
  
  public PageHeader(byte flags, short tupleSz,
                       short capacity, short freeOffset)
  {
    this.flags = flags;
    tupleSize = tupleSz;
    headerSize = getHeaderSize();
    bufCapacity = capacity;
    freeSpaceOffset = freeOffset;
  } 

  public PageHeader(PageHeader other) {
    flags = other.flags;
    tupleSize = other.tupleSize;
    headerSize = other.headerSize;
    bufCapacity = other.bufCapacity;
    freeSpaceOffset = other.freeSpaceOffset;
  }
  
  // Resets the used extent of the page.
  public void resetHeader() {
    freeSpaceOffset = filledBackward()? bufCapacity : headerSize;
  } 

  public void writeHeader(ChannelBuffer buf) {
    buf.writeByte(flags);
    buf.writeShort(tupleSize);
    buf.writeShort(bufCapacity);
    buf.writeShort(freeSpaceOffset);
  }
  
  // Returns the size of the disk-based header repr, in bytes.
  public short getHeaderSize() {
    return Integer.valueOf(1+((Short.SIZE>>3)*4)).shortValue();
  }

  public short getTupleSize() { return tupleSize; }
  
  public boolean isValidTupleSize(short size) {
    return tupleSize <= 0 || tupleSize == size;
  }

  public boolean isSpaceAvailable(short size) {
    return isValidTupleSize(size) && getFreeSpace() > size;
  }

  // Return the available space in the page, in bytes. 
  public short getFreeSpace() {
    return (short) (filledBackward()?
        (freeSpaceOffset - headerSize) : (bufCapacity - freeSpaceOffset));
  }

  // Offset of the first tuple in the data segment.
  public short getDataOffset() {
    return (short) (filledBackward()?
        (tupleSize <= 0? bufCapacity : bufCapacity - tupleSize) : headerSize);
  }
  
  // Offset of the freespace segment.
  public short getFreeSpaceOffset() { return freeSpaceOffset; }
  
  // Page capacity.
  public short getCapacity() { return bufCapacity; }

  // Current space usage.
  public short getUsedSpace() { 
    return (short) (filledBackward()?
        (bufCapacity - freeSpaceOffset) : (freeSpaceOffset - headerSize));
  }


  // Flag accessors
  public boolean isFlagSet(byte flagIndex) {
    return (flags & flagIndex) == flagIndex;
  }
  
  public void setFlag(byte flagIndex, boolean d) {
    if ( d ) flags |= flagIndex;
    else flags &= ~flagIndex;
  }

  // Dirty flag accessors.

  public boolean isDirty() { return isFlagSet(DIRTY); } 
  
  public void setDirty(boolean d) { setFlag(DIRTY, d); }

  // Forward fill accessors.
  
  public boolean filledBackward() { return isFlagSet(FILL_BACKWARD); }
  
  public void setFillDirection(boolean f) { setFlag(FILL_BACKWARD, f); }
  
  // Direction aware offset accessor.
  public short getPrevTupleOffset(short offset) {
    return (short) (tupleSize <= 0 ? INVALID_OFFSET :
      (filledBackward()? offset + tupleSize : offset - tupleSize));
  }

  // For forward filling, length must be that of the previous tuple.
  // For backward filling, length must be that of the current tuple.
  public short getPrevTupleOffset(short offset, short length) {
    short r = INVALID_OFFSET;
    if ( tupleSize <= 0 ) {
      r = (short) (filledBackward()? offset + length : offset - length);
    } else if ( length == tupleSize ) {
      r = getPrevTupleOffset(offset);
    }
    return r;
  }
  
  public short getNextTupleOffset(short offset) {
    return (short) (tupleSize <= 0 ? INVALID_OFFSET :
        (filledBackward()? offset - tupleSize : offset + tupleSize));
  }
  
  // For forward filling, length must be that of the current tuple.
  // For backward filling, length must be that of the next tuple.
  public short getNextTupleOffset(short offset, short length) {
    short r = INVALID_OFFSET;
    if ( tupleSize <= 0 ) {
      r = (short) (filledBackward()? offset - length : offset + length);
    } else if ( length == tupleSize ) {
      r = getPrevTupleOffset(offset);
    }
    return r;
  }
  
  // Free space index management.
  void advanceForward(short length) {
    if ( freeSpaceOffset + length < bufCapacity )
      freeSpaceOffset += length;
  }
  
  void advanceBackward(short length) {
    if ( freeSpaceOffset - length >= headerSize )
      freeSpaceOffset -= length;      
  }

  public void useSpace(short length) {
    if ( filledBackward() ) advanceBackward(length);
    else advanceForward(length);
  }

  public void freeSpace(short length) {
    if ( filledBackward() ) advanceForward(length);
    else advanceBackward(length);
  }

  public String toString() {
    return "ts: " + tupleSize
            + ", hs: " + headerSize
            + ", fso: " + freeSpaceOffset
            + ", cap: " + bufCapacity;
  }
}
