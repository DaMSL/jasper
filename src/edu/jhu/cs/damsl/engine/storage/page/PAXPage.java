package edu.jhu.cs.damsl.engine.storage.page;

import org.jboss.netty.buffer.ChannelBuffer;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.iterator.page.StorageIterator;
import edu.jhu.cs.damsl.engine.storage.page.factory.HeaderFactory;
import edu.jhu.cs.damsl.utils.hw1.HW1.*;

@CS416Todo
public class PAXPage extends Page<PAXPageHeader> {

  public PAXPage(Integer id, ChannelBuffer buf, Schema sch, byte flags) {
    this(new PageId(id), buf, sch, flags);
  }

  public PAXPage(PageId id, ChannelBuffer buf, Schema sch, byte flags) {
    super(id, buf, sch, flags);
  }

  // Factory accessors
  @CS416Todo
  public HeaderFactory<PAXPageHeader> getHeaderFactory() {
  	return null;
  }

  // Tuple accessors.
  
  // The default tuple retrieval method is via iteration.
  @CS416Todo
  public StorageIterator iterator() { return null; }

  // Append a variable-length tuple to the page.
  @CS416Todo  
  public boolean putTuple(Tuple t, short requestedSize) { return false; }

  // Append a fixed-size tuple to the page.
  @CS416Todo  
  public boolean putTuple(Tuple t) { return false; }

}