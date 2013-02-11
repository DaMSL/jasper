package edu.jhu.cs.damsl.engine.storage.page.factory;

import java.io.DataInput;
import java.io.FileNotFoundException;
import java.io.IOException;

import edu.jhu.cs.damsl.catalog.Schema;
import org.jboss.netty.buffer.ChannelBuffer;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPageHeader;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPageHeader.Slot;
import edu.jhu.cs.damsl.engine.storage.page.factory.HeaderFactory;
import edu.jhu.cs.damsl.engine.storage.page.factory.PageHeaderFactory;

public class SlottedPageHeaderFactory
				implements HeaderFactory<SlottedPageHeader>
{
  PageHeaderFactory pageHeaderFactory;

  public SlottedPageHeaderFactory() {
	pageHeaderFactory = new PageHeaderFactory();
  }
  
  public SlottedPageHeader getHeader(Schema sch, ChannelBuffer buf, byte flags) {
  	return new SlottedPageHeader(sch, buf, flags);
  }

  // Read the header from the backing buffer into the in-memory header.
  public SlottedPageHeader readHeader(ChannelBuffer buf) {
    PageHeader h = pageHeaderFactory.readHeader(buf);
    short slotCapacity = buf.readShort();
    short slotIndex = slotCapacity > 0? buf.readShort() : -1;
    short actualSlots = slotCapacity <= 0? buf.readShort() : slotCapacity;
    
    SlottedPageHeader r = new SlottedPageHeader(h, slotCapacity, slotIndex);

    short offset = 0;
    for ( short i = 0; i < actualSlots; ++i) {
      Slot s = r.new Slot(buf.readShort(), buf.readShort());
      r.setSlot(i, s);
      short noffset = (short) (s.offset + s.length);
      offset = offset > noffset? offset : noffset;
    }
    if ( offset > 0 ) { r.useSpace((short) (offset-r.getFreeSpaceOffset())); }
    return r;
  }
  
  public SlottedPageHeader readHeaderDirect(DataInput f)
      throws IOException
  {
    PageHeader h = pageHeaderFactory.readHeaderDirect(f);
    short slotCapacity = f.readShort();
    short slotIndex = slotCapacity > 0? f.readShort() : -1;
    short actualSlots = slotCapacity <= 0? f.readShort() : slotCapacity;
    
    SlottedPageHeader r = new SlottedPageHeader(h, slotCapacity, slotIndex);

    short offset = 0;
    for ( short i = 0; i < actualSlots; ++i) {
      Slot s = r.new Slot(f.readShort(), f.readShort());
      r.setSlot(i, s);
      short noffset = (short) (s.offset + s.length);
      offset = offset > noffset? offset : noffset;
    }
    if ( offset > 0 ) { r.useSpace((short) (offset-r.getFreeSpaceOffset())); }
    return r;
  }
}