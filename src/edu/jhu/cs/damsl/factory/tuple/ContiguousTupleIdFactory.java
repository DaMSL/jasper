package edu.jhu.cs.damsl.factory.tuple;

import org.jboss.netty.buffer.ChannelBuffer;

import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.tuple.ContiguousTupleId;
import edu.jhu.cs.damsl.factory.tuple.TupleIdFactory;

public class ContiguousTupleIdFactory extends TupleIdFactory<ContiguousTupleId>
{
  public ContiguousTupleIdFactory() {}

  public ContiguousTupleId getTupleId(ChannelBuffer buf) {
    PageId id = PageId.read(buf);
    short len = buf.readShort();
    short idx = buf.readShort();
    return new ContiguousTupleId(id, len, idx);
  }
}