package edu.jhu.cs.damsl.engine.storage.page;

import org.jboss.netty.buffer.ChannelBuffer;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.utils.hw1.HW1.*;

@CS416Todo
public class PAXPageHeader extends PageHeader {

  public PAXPageHeader(Schema sch, ChannelBuffer buf, byte flags) {
  	super(sch, buf, flags);
  }
}