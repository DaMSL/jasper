package edu.jhu.cs.damsl.engine.storage.page;

import org.jboss.netty.buffer.ChannelBuffer;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.tuple.SlottedTupleId;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.iterator.page.SlottedPageIterator;
import edu.jhu.cs.damsl.factory.page.HeaderFactory;
import edu.jhu.cs.damsl.factory.page.SlottedPageHeaderFactory;
import edu.jhu.cs.damsl.factory.tuple.TupleIdFactory;
import edu.jhu.cs.damsl.factory.tuple.SlottedTupleIdFactory;

public class SlottedPage extends Page<SlottedTupleId, SlottedPageHeader> {

  public static final HeaderFactory<SlottedPageHeader> headerFactory 
    = new SlottedPageHeaderFactory();

  public static final TupleIdFactory<SlottedTupleId> tupleIdFactory
    = new SlottedTupleIdFactory();

  // Commonly used constructor.
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

  @Override
  public TupleIdFactory<SlottedTupleId> getTupleIdFactory() {
    return tupleIdFactory;
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

  @Override
  public SlottedPageIterator
  iterator(SlottedTupleId start, SlottedTupleId end) {
    return new SlottedPageIterator(getId(), this, start, end);
  }

  // Retrieves the tuple corresponding to the given identifier.
  @Override
  public Tuple getTuple(SlottedTupleId id) {
    Tuple r = null;
    int slotIndex   = id.slot();
    short tupleSize = id.tupleSize();

    if ( header.isValidTuple(slotIndex) ) {
      int offset = header.getSlotOffset(slotIndex);
      int length = header.getSlotLength(slotIndex);

      ChannelBuffer data = slice(offset, length);

      if ( tupleSize > 0 && tupleSize == length) {
        r = Tuple.getTuple(data, tupleSize, length); 
      } else if ( tupleSize <= 0 ) {
        r = Tuple.getTuple(data, length);
      } else {
        logger.warn("tuple size and slot length mismatch {} vs {}",
            tupleSize, length);
      }
    }
          
    return r;    
  }

  // Adds a tuple the to start of the free space block.
  // Must update the header before actually writing data to correctly
  // set the free space pointer.
  protected SlottedTupleId putTuple(Tuple t, short actualTupleSize)
  {
    SlottedTupleId r = null;
    int nextSlot = header.useNextSlot(actualTupleSize);
    if ( header.isValidSlot(nextSlot) ) {
      setBytes(header.getSlotOffset(nextSlot), t, 0, actualTupleSize);
      setDirty(true);
      r = new SlottedTupleId(getId(), actualTupleSize, nextSlot);
    }
    return r;
  }
  
  // Appends a tuple to the page, handling both fixed and variable-length tuples.
  @Override
  public SlottedTupleId putTuple(Tuple t) {
    SlottedTupleId r = null;
    short tupleSize = Integer.valueOf(t.size()).shortValue();
    if ( header.isValidTupleSize(tupleSize) ) {
      r = putTuple(t, tupleSize);
    }
    return r;  
  }

  // Inserts a tuple at the given slot in this page, overwriting the existing
  // entry for fixed length tuples. For variable length tuples, if the
  // existing entry does not contain sufficient space, that space becomes
  // garbage and the tuple is inserted at the free space offset.
  protected boolean insertTuple(SlottedTupleId id, Tuple t, short actualTupleSize) {
    boolean valid = false;
    int slotIndex = id.slot();
    if ( header.isValidSlot(slotIndex) ) {
      // Check if there is sufficient space in the tuple.
      if ( header.getSlotLength(slotIndex) >= actualTupleSize ) {
        // Update the slot length to the tuple size and write the tuple in place.
        short slotOffset = header.getSlotOffset(slotIndex);
        header.setSlot(slotIndex, slotOffset, actualTupleSize);
        setBytes(slotOffset, t, 0, actualTupleSize);
        setDirty(true);
        valid = true;
      } else {
        // Add the tuple at the free space offset.
        valid = header.useSlot(slotIndex, actualTupleSize);
        if ( valid ) {
          setBytes(header.getSlotOffset(slotIndex), t, 0, actualTupleSize);
          setDirty(true);        
        }
      }
    }
    return valid;
  }
  
  // Insert a tuple at the exact location given by the tuple identifier.
  @Override
  public boolean insertTuple(SlottedTupleId id, Tuple t) {
    short tupleSize = Integer.valueOf(t.size()).shortValue();;
    return header.isValidTupleSize(tupleSize)
            && insertTuple(id, t, header.getTupleSize());
  }

  // Zeroes out the contents of the given slot.
  protected void clearTuple(SlottedTupleId id) {
    int slotIndex = id.slot();
    setZero(header.getSlotOffset(slotIndex), header.getSlotLength(slotIndex));
    setDirty(true);
  }

  // Removes the tuple at the given slot in this page, zeroing the tuple data.
  @Override
  public boolean removeTuple(SlottedTupleId id) {
    boolean r = false;
    int slotIndex = id.slot();
    if ( r = header.isValidTuple(slotIndex) ) {
      clearTuple(id); // Sets the page as dirty.
      header.resetSlot(slotIndex);
    }
    return r;
  }

  // Removes all tuples from the page, zeroing out their content.
  @Override
  public void removeTuples() {
    // Zero out previous contents. This isn't strictly necessary, and we
    // could improve performance by avoiding this.
    short used = header.getUsedSpace();
    short start = header.filledBackward()?
        header.getFreeSpaceOffset() : header.getHeaderSize();
    setZero(start, used);
    header.freeSpace(used);

    // Reset header, including slots.
    header.resetHeader();
    setDirty(true);
  }

}
