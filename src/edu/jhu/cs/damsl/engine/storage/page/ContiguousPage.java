package edu.jhu.cs.damsl.engine.storage.page;

import org.jboss.netty.buffer.ChannelBuffer;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.tuple.ContiguousTupleId;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.iterator.page.ContiguousPageIterator;
import edu.jhu.cs.damsl.factory.page.PageFactory;
import edu.jhu.cs.damsl.factory.page.HeaderFactory;
import edu.jhu.cs.damsl.factory.page.PageHeaderFactory;
import edu.jhu.cs.damsl.factory.tuple.TupleIdFactory;
import edu.jhu.cs.damsl.factory.tuple.ContiguousTupleIdFactory;

public class ContiguousPage extends Page<ContiguousTupleId, PageHeader> {

  public static final HeaderFactory<PageHeader> headerFactory 
    = new PageHeaderFactory();

  public static final TupleIdFactory<ContiguousTupleId> tupleIdFactory
    = new ContiguousTupleIdFactory();

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
  public TupleIdFactory<ContiguousTupleId> getTupleIdFactory() {
    return tupleIdFactory;
  }


  @Override
  public ContiguousPageIterator iterator() {
    return new ContiguousPageIterator(getId(), this);
  }

  @Override
  public ContiguousPageIterator
  iterator(ContiguousTupleId start, ContiguousTupleId end) {
    return new ContiguousPageIterator(getId(), this, start, end);
  }
  
  protected boolean validTupleBoundary(short offset) {
    short testOffset = header.filledBackward()?
        header.getFreeSpaceOffset() : header.getHeaderSize();
    short tupSz = header.getTupleSize();
    return ((offset-testOffset) % tupSz) == 0;
  }
  
  public boolean isValidData(short offset, short length) {
    if ( length <= 0 ) return false;

    short lower = header.filledBackward()?
        header.getFreeSpaceOffset() : header.getHeaderSize();
    
    short upper = header.filledBackward()?
        header.getCapacity() : header.getFreeSpaceOffset();
    
    return ( lower <= offset && offset+length <= upper
              && validTupleBoundary(offset) );
  }
  
  public boolean isValidOffset(short offset) {
    short lower = header.filledBackward()?
        header.getFreeSpaceOffset() : header.getHeaderSize();
    
    short upper = header.filledBackward()?
        header.getCapacity() : header.getFreeSpaceOffset();

    return ( lower <= offset && offset < upper
              && validTupleBoundary(offset) );
  }

  // Helper method for getTuple.
  // Returns a channel buffer spanning the given range, checking if the
  // range is a valid data region.
  protected ChannelBuffer getBuffer(short offset, short length) {
    ChannelBuffer r = null;
    if ( isValidData(offset, length) ) {
      r = header.filledBackward()?
            slice(offset-length, length) : slice(offset, length);
    }
    return r;
  }

  // Get a specific tuple from the page
  @Override
  public Tuple getTuple(ContiguousTupleId id) {
    Tuple r = null;
    
    short tupleOffset = id.offset();
    short tupleSize   = id.tupleSize();
    short boundary    = header.getFreeSpaceOffset();

    if ( header.isValidTupleSize(tupleSize) ) {
      // If we're asking for a variable length, or unknown length tuple,
      // read the tuple considering all bytes to the end of the valid data region.
      if ( tupleSize <= 0 ) {
        tupleSize = (short) (header.filledBackward()?
                        tupleOffset - boundary : boundary - tupleOffset);
      }
      ChannelBuffer data = getBuffer(tupleOffset, tupleSize);
      if ( data != null ) { r = Tuple.getTuple(data, tupleSize, tupleSize); }
    }
    return r;
  }

  // Helper method for putTuple.
  // Returns the offset at which the given buffer is written to this page.
  protected short putBuffer(ChannelBuffer buf, short length) {
    short offset = -1;
    boolean available =
      header.isValidTupleSize(length) && length < header.getFreeSpace();

    if ( available ) {
      if ( header.filledBackward() ) header.useSpace(length);
      offset = header.getFreeSpaceOffset();
      setBytes(offset, buf, 0, length);
      if ( !header.filledBackward() ) header.useSpace(length);
      setDirty(true);
    }
    return offset;
  }

  // Append a tuple to the page, supporting both fixed and variable-length tuples.
  @Override
  public ContiguousTupleId putTuple(Tuple t) {
    ContiguousTupleId r = null;    

    short tupleSize = Integer.valueOf(
      t.isFixedLength()? t.getFixedLength() : t.size()).shortValue();

    if ( header.isValidTupleSize(tupleSize) ) {
      short offset = putBuffer(t, tupleSize);
      if ( offset >= 0 ) { r = new ContiguousTupleId(getId(), tupleSize, offset); }
    }
    return r;
  }

  // Helper method for insertTuple.
  // Shifts all data starting at the given offset by the desired amount.
  // This method handles both forward and backward filled buffers.
  // For both fill directions, offset is the left-most byte of the relevant
  // region of data.
  protected void shiftBuffer(short offset, short shift) {
    boolean available = shift < header.getFreeSpace();
    
    // Shifted data must be a integer multiple of tuple size.
    if ( available && (shift % header.getTupleSize()) == 0 ) {
      boolean forward = !header.filledBackward();
      short step = shift;

      // Last is the offset to which we will copy data. This offset will
      // be adjusted until it meets the starting offset of the data segment
      // to be moved.
      short last = (short) (forward?
        header.getFreeSpaceOffset() : (header.getFreeSpaceOffset()-step));
      
      // First is the offset of the final byte to be shifted, according
      // to the fill direction. For backward filling, we want the full tuple
      // addressed by the given offset to be moved.
      short first = (short) (forward? offset : offset+header.getTupleSize());
      
      for (; (forward? last-step >= first : last+step <= first);) {
        setBytes(last, this, forward? last-step : last+step, step);
        if ( forward ) last -= step;
        else last += step;
      }
      header.useSpace(shift);
      setDirty(true);
    }
  }
  
  // Add a buffer at the given offset, shifting existing data.
  protected boolean insertBuffer(short offset, ChannelBuffer buf, short length) {
    boolean r = false;
    if ( offset == header.getFreeSpaceOffset() ) {
      putBuffer(buf, length);
      r = true;
    } else if ( isValidOffset(offset) ) {
      shiftBuffer(offset, length);
      setBytes(offset, buf, 0, length);
      r = true;
    }
    return r;
  }
  
  // Insert a tuple at the exact location given by the tuple identifier.
  @Override
  public boolean insertTuple(ContiguousTupleId id, Tuple t) {
    short tupleSize = Integer.valueOf(
      t.isFixedLength()? t.getFixedLength() : t.size()).shortValue();
    return header.isValidTupleSize(tupleSize)
            && insertBuffer(id.offset(), t, tupleSize);
  }

  // Removes 'length' bytes from this page starting at the given offset.
  // Any bytes in the data region beyond offset+length are shifted to offset, thereby
  // automatically compacting the removed region.
  // If the length parameter is negative, this method removes all bytes from the
  // offset to the free space offset. That is it removes all bytes in the data region
  // of this page beyond the offset.
  protected boolean removeBuffer(short offset, short length) {
    boolean r = false;
    boolean forward = !header.filledBackward();
    short boundary = header.getFreeSpaceOffset();

    short remaining = (short) (length <= 0?
        (forward? boundary - offset : offset - boundary) : length);

    if ( isValidData(offset, remaining) ) {
      // Zero and shift remaining bytes.
      setZero(offset, remaining);
      if ( length > 0 &&
           (forward? (offset+length < boundary) : (boundary <= offset) ) )
      {
        int shiftLength = forward?
            (boundary - (offset+length)) : (offset - boundary);
            
        int start = forward? offset : boundary+length;
        
        ChannelBuffer shiftData = forward?
          slice(offset+length, shiftLength) : slice(boundary, shiftLength);
        
        setBytes(start, shiftData, shiftLength);
        header.freeSpace(length);

      } else {
        header.freeSpace(remaining);
      }
      setDirty(true);

      r = true;
    }
    return r;
  }

  // Remove a specific tuple from this page.
  @Override
  public boolean removeTuple(ContiguousTupleId id) {
    boolean r = false;
    short tupleSize = id.tupleSize();
    short offset    = id.offset();
    short boundary  = header.getFreeSpaceOffset();

    if ( header.isValidTupleSize(tupleSize) ) {
      // Attempt to read the tuple size from the page if we don't have a valid one.
      if ( tupleSize <= 0 ) {
        short length = (short) (header.filledBackward()?
                        offset - boundary : boundary - offset);
        
        ChannelBuffer data = slice(offset, length);
        
        tupleSize = Integer.valueOf(
          Tuple.getTuple(data, length, length).size()).shortValue(); 
      }

      if ( isValidData(offset, tupleSize) ) {
        removeBuffer(offset, tupleSize);
        r = true;
      }
    }
    return r;
  }

  // Removes all tuples from the page.
  @Override
  public void removeTuples() {
    short used = header.getUsedSpace();
    short start = header.filledBackward()?
        header.getFreeSpaceOffset() : header.getHeaderSize();
    setZero(start, used);
    header.freeSpace(used);
    setDirty(true);
  }

  /* OLD REMOVE implementation
  protected void removeBuffer(short offset, short removeLength, short dataLength) {
    boolean forward = !header.filledBackward();
    int boundary = header.getFreeSpaceOffset();
    setZero(offset, removeLength);

    // Shift remaining bytes.
    if ( dataLength > 0 &&
         (forward? (offset+dataLength < boundary) : (boundary <= offset) ) )
    {
      int shiftLength = forward?
          (boundary - (offset+dataLength)) : (offset - boundary);
          
      int start = forward? offset : boundary+dataLength;
      
      ChannelBuffer shiftData = forward?
          slice(offset+dataLength, shiftLength) :
          slice(boundary, shiftLength);
      
      setBytes(start, shiftData, shiftLength);
      header.freeSpace(dataLength);

    } else {
      header.freeSpace(removeLength);
    }
    setDirty(true);
  }

  public boolean removeTuple(short offset, short length) {
    boolean r = false;
    int boundary = header.getFreeSpaceOffset();

    short remaining = (short) (length <= 0?
        (header.filledBackward()? offset - boundary : boundary - offset) : length);

    if ( isValidData(offset, remaining) ) {
      removeBuffer(offset, (short) remaining, length);
      r = true;
    }
    return r;
  }

  public boolean removeTuple(short offset) {
    boolean r = false;
    int tupleSize = header.getTupleSize();
    int boundary = header.getFreeSpaceOffset();

    short length = (short) (tupleSize <= 0?
        (header.filledBackward()? offset - boundary : boundary - offset)
        : tupleSize);

    if ( isValidData(offset, length) ) {
      if ( tupleSize <= 0 ) {
        ChannelBuffer data = slice(offset, length);
        length = (short) (Tuple.getTuple(data, length, length).size()); 
      }
      removeBuffer(offset, (short) length, (short) length);
      r = true;
    }
    return r;
  }
  */
}
