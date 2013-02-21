package edu.jhu.cs.damsl.engine.storage.page.factory;

import java.io.FileNotFoundException;

import org.jboss.netty.buffer.ChannelBuffer;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.engine.storage.page.factory.PageFactory;
import edu.jhu.cs.damsl.engine.storage.page.ContiguousPage;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;

public class ContiguousPageFactory implements PageFactory<PageHeader, ContiguousPage>
{
  Schema schema;
  byte flags;

  public ContiguousPageFactory() {}
  
  public ContiguousPageFactory(Schema sch, byte fl) { setConfiguration(sch, fl); }

  public void setConfiguration(Schema sch, byte fl) {
    schema = sch;
    flags = fl;
  }

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

  public ContiguousPage getPage(Integer id, ChannelBuffer buf) {
    return new ContiguousPage(id, buf, schema, flags);
  }
  
  public ContiguousPage getPage(PageId id, ChannelBuffer buf) {
    return new ContiguousPage(id, buf, schema, flags);
  }

}