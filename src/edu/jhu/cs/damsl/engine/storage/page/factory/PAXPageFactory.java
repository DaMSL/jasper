package edu.jhu.cs.damsl.engine.storage.page.factory;

import java.io.FileNotFoundException;

import org.jboss.netty.buffer.ChannelBuffer;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.engine.storage.page.factory.PageFactory;
import edu.jhu.cs.damsl.engine.storage.page.PAXPage;
import edu.jhu.cs.damsl.engine.storage.page.PAXPageHeader;
import edu.jhu.cs.damsl.utils.hw1.HW1.*;

@CS416Todo
public class PAXPageFactory implements PageFactory<PAXPageHeader, PAXPage>
{
  public PAXPage getPage(Integer id, ChannelBuffer buf, Schema sch, byte flags) {
    return null;
  }

  public PAXPage getPage(PageId id, ChannelBuffer buf, Schema sch, byte flags) {
    return null;
  }

  public PAXPage getPage(Integer id, ChannelBuffer buf, Schema sch) {
    return null;
  }
  
  public PAXPage getPage(PageId id, ChannelBuffer buf, Schema sch) {
    return null;
  }

  public PAXPage getPage(Integer id, ChannelBuffer buf, byte flags) {
    return null;
  }
  
  public PAXPage getPage(PageId id, ChannelBuffer buf, byte flags) {
    return null;
  }

  public PAXPage getPage(Integer id, ChannelBuffer buf) {
    return null;
  }
  
  public PAXPage getPage(PageId id, ChannelBuffer buf) {
    return null;
  }

}