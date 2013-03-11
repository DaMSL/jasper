package edu.jhu.cs.damsl.factory.page;

import java.io.FileNotFoundException;

import org.jboss.netty.buffer.ChannelBuffer;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.tuple.ContiguousTupleId;
import edu.jhu.cs.damsl.engine.storage.page.ContiguousPage;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.factory.page.PageFactory;

public class ContiguousPageFactory implements
                PageFactory<ContiguousTupleId, PageHeader, ContiguousPage>
{
  public ContiguousPage getPage(Integer id, ChannelBuffer buf, Schema sch, byte flags) {
  	return new ContiguousPage(id, buf, sch, flags);
  }

  public ContiguousPage getPage(PageId id, ChannelBuffer buf, Schema sch, byte flags) {
	  return new ContiguousPage(id, buf, sch, flags);
  }

  public ContiguousPage getPage(Integer id, ChannelBuffer buf, Schema sch) {
	  return new ContiguousPage(id, buf, sch);
  }
  
  public ContiguousPage getPage(PageId id, ChannelBuffer buf, Schema sch) {
	  return new ContiguousPage(id, buf, sch);
  }

  public ContiguousPage getPage(Integer id, ChannelBuffer buf, byte flags) {
	  return new ContiguousPage(id, buf, flags);
  }
  
  public ContiguousPage getPage(PageId id, ChannelBuffer buf, byte flags) {
	  return new ContiguousPage(id, buf, flags);
  }

}