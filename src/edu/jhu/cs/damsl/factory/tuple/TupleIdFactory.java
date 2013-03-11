package edu.jhu.cs.damsl.factory.tuple;

import org.jboss.netty.buffer.ChannelBuffer;
import edu.jhu.cs.damsl.catalog.identifiers.TupleId;

public abstract class TupleIdFactory<IdType extends TupleId> {
  public abstract IdType getTupleId(ChannelBuffer buf);
}