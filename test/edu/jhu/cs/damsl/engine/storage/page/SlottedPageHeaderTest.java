package edu.jhu.cs.damsl.engine.storage.page;

import java.util.LinkedList;
import java.util.ListIterator;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.HeapChannelBufferFactory;

import edu.jhu.cs.damsl.catalog.Defaults;
import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.file.SlottedHeapFile;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPageHeader;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPageHeader.Slot;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPage;
import edu.jhu.cs.damsl.engine.storage.page.factory.SlottedPageHeaderFactory;
import edu.jhu.cs.damsl.utils.CommonTestUtils;

public class SlottedPageHeaderTest {

  static final int capacity =
    Defaults.getSizeAsInteger(Defaults.defaultPageSize, Defaults.defaultPageUnit);

  SlottedPageHeader header;
  Schema schema;

  @Before
  public void setUp() {
    ChannelBuffer buf = HeapChannelBufferFactory.getInstance().getBuffer(capacity);
    
    schema = CommonTestUtils.getLIDSchema();
    header = new SlottedPageHeader(schema, buf, (byte) 0x0);
  }

  void populateHeader() {
    if ( !(header == null || schema == null)  ) {
      short tupleSize = Integer.valueOf(schema.getTupleSize()+Tuple.headerSize).shortValue();
      for (int i = header.getNumSlots()-1; i >= 0; --i) {
        header.useNextSlot(tupleSize);
      }
    }
  }

  /** Slot tests */

  @Test
  public void useSlotTest() { populateHeader(); assertTrue ( true ); }

  // Checks every slot is at the expected offset.
  @Test
  public void getSlotTest() { 
    populateHeader();
    int dataStart = header.getHeaderSize();
    int tupleSize = schema.getTupleSize()+Tuple.headerSize;
    for (int i = header.getNumSlots()-1; i >= 0; --i) {
      Slot s = header.getSlot(i);
      short expectedOffset = Integer.valueOf(dataStart+(i*tupleSize)).shortValue();
      assertTrue( s != null && 
                  s.offset == expectedOffset && s.length == tupleSize);
    }
  }

  // Reverses the slot directory using setSlot, and checks that each
  // slot is updated correctly.
  @Test
  public void setSlotTest() { 
    populateHeader();
    LinkedList<Slot> slots = new LinkedList<Slot>();
    for (int i = header.getNumSlots()-1; i >= 0; --i) {
      slots.push(header.getSlot(i));
    }

    int index = 0;
    ListIterator<Slot> it = slots.listIterator();
    while (it.hasNext() && index < header.getNumSlots()) {
      Slot s = it.next();
      header.setSlot(index, s);
      assert ( header.getSlot(index) == s );
    }
  }

  // Resets a populated header, and checks if resetting preserves the
  // number of slots, while unwinding the next available slot index.
  @Test
  public void resetTest() { 
    populateHeader();
    int slotsBefore = header.getNumSlots();
    header.resetHeader();
    assertTrue ( header.getNumSlots() == slotsBefore && header.getNextSlot() == 0 );
  }

  @Test
  public void growSlotTest() { 
    if ( header.hasDynamicSlots() ) {
      populateHeader();
      int slotsBefore = header.getNumSlots();
      header.growSlots(slotsBefore+1, header.new Slot(SlottedPageHeader.INVALID_SLOT, (short) -1));
      assertTrue ( header.getNumSlots() == slotsBefore+2 );
    }
  }


  /** Generic header tests */

  @Test
  public void headerSizeTest() {
    populateHeader();
    int expectedSize = (1+(Short.SIZE>>3)*5)+
                        header.getNumSlots()*SlottedPageHeader.SLOT_SIZE;
    assertTrue ( header.getHeaderSize() == expectedSize );
  }

  @Test
  public void spaceAvailableTest() {
    populateHeader();
    short tupleSize = Integer.valueOf(schema.getTupleSize()+Tuple.headerSize).shortValue();
    assertFalse ( header.isSpaceAvailable(tupleSize) );
  }

  // Write the header to a heap-based channel buffer, and check that
  // the write transferred the number of bytes given by getHeaderSize.
  @Test
  public void writeHeaderTest() { 
    populateHeader();
    ChannelBuffer buf = HeapChannelBufferFactory.getInstance().getBuffer(capacity);
    header.writeHeader(buf);
    assertTrue ( header.getHeaderSize() == buf.readableBytes() );
  }

  // Write the header to a heap-based channel buffer, and then read in a
  // new header from that buffer. Check if the two headers have the same
  // number of slots and slot contents.
  @Test
  public void readHeaderTest() {
    populateHeader();
    ChannelBuffer buf = HeapChannelBufferFactory.getInstance().getBuffer(capacity);
    header.writeHeader(buf);

    SlottedPageHeaderFactory f = new SlottedPageHeaderFactory();
    SlottedPageHeader newHeader = f.readHeader(buf);
    assertTrue ( header.getNumSlots() == newHeader.getNumSlots() );
    for (int i = 0; i < header.getNumSlots(); ++i) {
      Slot a = header.getSlot(i);
      Slot b = newHeader.getSlot(i);
      assertTrue ( a.offset == b.offset && a.length == b.length );
    }
  }

}