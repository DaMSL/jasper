package edu.jhu.cs.damsl.factory.page;

import java.io.DataInput;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.factory.page.HeaderFactory;

public class PageHeaderFactory implements HeaderFactory<PageHeader>
{
  public PageHeaderFactory() {}
  
  public PageHeader getHeader(Schema sch, ChannelBuffer buf, byte flags) {
  	return new PageHeader(sch, buf, flags);
  }

  // Reading and writing headers updates the indexes of the given buffer.
  // The caller must determine whether to save and restore indexes.
  public PageHeader readHeader(ChannelBuffer buf) {
    byte flags = buf.readByte();
    short tupleSize = buf.readShort();
    short capacity = buf.readShort();
    short freeSpaceOffset = buf.readShort();

    PageHeader r = new PageHeader(flags, tupleSize, capacity, freeSpaceOffset);
    return r;
  }

  public PageHeader readHeaderDirect(DataInput buf) throws IOException {
    byte flags = buf.readByte();
    short tupleSize = buf.readShort();
    short capacity = buf.readShort();
    short freeSpaceOffset = buf.readShort();

    PageHeader r = new PageHeader(flags, tupleSize, capacity, freeSpaceOffset);
    return r;
  }
}