package edu.jhu.cs.damsl.engine.storage.page;

import java.util.Random;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.HeapChannelBufferFactory;

import edu.jhu.cs.damsl.catalog.Defaults;
import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.engine.storage.page.factory.PageHeaderFactory;
import edu.jhu.cs.damsl.utils.CommonTestUtils;

public class PageHeaderTest {

  static final int capacity =
    Defaults.getSizeAsInteger(Defaults.defaultPageSize, Defaults.defaultPageUnit);

  Random rng;
  Schema schema;
  PageHeader header;

  @Before
  public void setUp() {
    ChannelBuffer buf = HeapChannelBufferFactory.getInstance().getBuffer(capacity);
    
    rng = new Random(12345);
    schema = CommonTestUtils.getLIDSchema();
    header = new PageHeader(schema, buf, (byte) 0x0);    
  }

  int populateHeader() {
    int r = -1;
    if ( header != null ) {
      short tupleSize = header.getTupleSize();
      int maxTuples = (header.getFreeSpace() / tupleSize)-1;
      r = 1 + rng.nextInt(maxTuples);
      int i = r;
      while ( i > 0 && header.isSpaceAvailable(tupleSize) ) {
        i -= 1;
        header.useSpace(tupleSize);
        header.setDirty(true);
      }
    }
    return r;
  }

  @Test
  public void tupleSizeTest() {
    assertTrue ( header.getTupleSize() == schema.getTupleSize()+Tuple.headerSize);
  }

  @Test
  public void useSpaceTest() {
    int count = populateHeader();
    assertTrue ( header.getUsedSpace() == (count*header.getTupleSize()) );
  }

  @Test
  public void freeSpaceTest() { 
    int count = populateHeader();
    int expected = capacity - (header.getHeaderSize()+count*header.getTupleSize());
    assertTrue ( header.getFreeSpace() == expected );
  }

  @Test
  public void resetTest() { 
    int count = populateHeader();
    header.resetHeader();
    assertTrue ( header.getUsedSpace() == 0 );
  }

  @Test
  public void writeTest() { 
    int count = populateHeader();
    ChannelBuffer buf = HeapChannelBufferFactory.getInstance().getBuffer(capacity);
    header.writeHeader(buf);
    assertTrue ( header.getHeaderSize() == buf.readableBytes() );
  }

  @Test
  public void readTest() { 
    int count = populateHeader();
    ChannelBuffer buf = HeapChannelBufferFactory.getInstance().getBuffer(capacity);
    header.writeHeader(buf);

    PageHeaderFactory f = new PageHeaderFactory();
    PageHeader newHeader = f.readHeader(buf);
    assertTrue ( header.getFreeSpace() == newHeader.getFreeSpace() 
                  && header.isDirty() == newHeader.isDirty() );
  }

}