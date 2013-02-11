package edu.jhu.cs.damsl.engine.storage.page.factory;

import java.io.DataInput;
import java.io.FileNotFoundException;
import java.io.IOException;

import edu.jhu.cs.damsl.catalog.Schema;
import org.jboss.netty.buffer.ChannelBuffer;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.engine.storage.page.PAXPageHeader;
import edu.jhu.cs.damsl.engine.storage.page.factory.HeaderFactory;
import edu.jhu.cs.damsl.engine.storage.page.factory.PageHeaderFactory;
import edu.jhu.cs.damsl.utils.hw1.HW1.*;

@CS416Todo
public class PAXPageHeaderFactory
				implements HeaderFactory<PAXPageHeader>
{

  public PAXPageHeaderFactory() {}
  
  public PAXPageHeader getHeader(Schema sch, ChannelBuffer buf, byte flags) {
  	return null;
  }

  // Read the header from the backing buffer into the in-memory header.
  public PAXPageHeader readHeader(ChannelBuffer buf) {
    return null;
  }
  
  public PAXPageHeader readHeaderDirect(DataInput f)
      throws IOException
  {
    return null;
  }
}