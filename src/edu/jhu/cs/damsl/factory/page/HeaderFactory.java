package edu.jhu.cs.damsl.factory.page;

import java.io.DataInput;
import java.io.FileNotFoundException;
import java.io.IOException;

import edu.jhu.cs.damsl.catalog.Schema;
import org.jboss.netty.buffer.ChannelBuffer;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;

public interface HeaderFactory<HeaderType extends PageHeader>
{
  public HeaderType getHeader(Schema sch, ChannelBuffer buf, byte flags);

  public HeaderType readHeader(ChannelBuffer buf);
  public HeaderType readHeaderDirect(DataInput buf) throws IOException;
}