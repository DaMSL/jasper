package edu.jhu.cs.damsl.factory.tuple;

import org.jboss.netty.buffer.ChannelBuffer;

import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.tuple.SlottedTupleId;
import edu.jhu.cs.damsl.factory.tuple.TupleIdFactory;

public class SlottedTupleIdFactory extends TupleIdFactory<SlottedTupleId>
{
  public SlottedTupleIdFactory() {}

  public SlottedTupleId getTupleId(ChannelBuffer buf) {
    PageId id = PageId.read(buf);
    short len = buf.readShort();
    int idx   = buf.readInt();
    return new SlottedTupleId(id, len, idx);
  }
}