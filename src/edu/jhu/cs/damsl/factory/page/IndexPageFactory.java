package edu.jhu.cs.damsl.factory.page;

import java.io.FileNotFoundException;

import org.jboss.netty.buffer.ChannelBuffer;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TupleId;
import edu.jhu.cs.damsl.catalog.identifiers.tuple.ContiguousTupleId;
import edu.jhu.cs.damsl.engine.storage.index.IndexPage;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.factory.page.PageFactory;

public class IndexPageFactory<IdType extends TupleId>
                implements PageFactory<ContiguousTupleId, PageHeader, IndexPage<IdType>>
{
  public IndexPage<IdType> getPage(Integer id, ChannelBuffer buf, Schema sch, byte flags) {
    return new IndexPage<IdType>(id, buf, sch, flags);
  }

  public IndexPage<IdType> getPage(PageId id, ChannelBuffer buf, Schema sch, byte flags) {
    return new IndexPage<IdType>(id, buf, sch, flags);
  }

  public IndexPage<IdType> getPage(Integer id, ChannelBuffer buf, Schema sch) {
    return new IndexPage<IdType>(id, buf, sch);
  }
  
  public IndexPage<IdType> getPage(PageId id, ChannelBuffer buf, Schema sch) {
    return new IndexPage<IdType>(id, buf, sch);
  }

  public IndexPage<IdType> getPage(Integer id, ChannelBuffer buf, byte flags) {
    return new IndexPage<IdType>(id, buf, flags);
  }
  
  public IndexPage<IdType> getPage(PageId id, ChannelBuffer buf, byte flags) {
    return new IndexPage<IdType>(id, buf, flags);
  }

}