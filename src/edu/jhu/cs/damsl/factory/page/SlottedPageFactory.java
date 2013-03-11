package edu.jhu.cs.damsl.factory.page;

import java.io.FileNotFoundException;

import org.jboss.netty.buffer.ChannelBuffer;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.tuple.SlottedTupleId;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPageHeader;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPage;
import edu.jhu.cs.damsl.factory.page.PageFactory;

public class SlottedPageFactory implements
                PageFactory<SlottedTupleId, SlottedPageHeader, SlottedPage>
{
  public SlottedPage getPage(Integer id, ChannelBuffer buf, Schema sch, byte flags) {
  	return new SlottedPage(id, buf, sch, flags);
  }

  public SlottedPage getPage(PageId id, ChannelBuffer buf, Schema sch, byte flags) {
	  return new SlottedPage(id, buf, sch, flags);
  }

  public SlottedPage getPage(Integer id, ChannelBuffer buf, Schema sch) {
  	return new SlottedPage(id, buf, sch);
  }
  
  public SlottedPage getPage(PageId id, ChannelBuffer buf, Schema sch) {
  	return new SlottedPage(id, buf, sch);
  }

  public SlottedPage getPage(Integer id, ChannelBuffer buf, byte flags) {
  	return new SlottedPage(id, buf, flags);
  }
  
  public SlottedPage getPage(PageId id, ChannelBuffer buf, byte flags) {
  	return new SlottedPage(id, buf, flags);
  }

}