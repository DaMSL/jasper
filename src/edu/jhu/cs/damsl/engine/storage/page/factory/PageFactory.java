package edu.jhu.cs.damsl.engine.storage.page.factory;

import java.io.FileNotFoundException;

import org.jboss.netty.buffer.ChannelBuffer;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;

public interface PageFactory<HeaderType extends PageHeader, 
							 PageType extends Page<HeaderType>>
{
  public PageType getPage(Integer id, ChannelBuffer buf, Schema sch, byte flags);
  public PageType getPage(PageId id, ChannelBuffer buf, Schema sch, byte flags);

  public PageType getPage(Integer id, ChannelBuffer buf, Schema sch);
  public PageType getPage(PageId id, ChannelBuffer buf, Schema sch);

  public PageType getPage(Integer id, ChannelBuffer buf, byte flags);
  public PageType getPage(PageId id, ChannelBuffer buf, byte flags);

}