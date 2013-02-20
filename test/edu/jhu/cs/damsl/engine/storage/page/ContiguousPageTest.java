package edu.jhu.cs.damsl.engine.storage.page;

import static org.junit.Assert.*;

import java.util.List;
import java.util.ListIterator;

import org.junit.Before;
import org.junit.Test;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.page.ContiguousPage;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.utils.CommonTestUtils;
import edu.jhu.cs.damsl.utils.ContiguousPageTestUtils;

public class ContiguousPageTest {

  private ContiguousPageTestUtils ptUtils;
  private Schema schema;
  private List<Tuple> tuples;
  private boolean fillBackward = false;
  
  private final static int numTuples = 5;

  @Before
  public void setUp() {
    ptUtils = new ContiguousPageTestUtils();
    schema = CommonTestUtils.getIIISchema();
    tuples = ptUtils.getTuples(schema, numTuples);
  }

  // Check the header has the appropriate size for the LID schema
  @Test
  public void headerTest() {
    Schema sch = CommonTestUtils.getLIDSchema();

    ContiguousPage p = null;
    try {
      p = new ContiguousPage(-1, ptUtils.getPool().getPage(), sch);
    } catch (InterruptedException e) { e.printStackTrace(); }
    
    short expectedSize =
        (short) (((Long.SIZE+Integer.SIZE+Double.SIZE)>>3)+Tuple.headerSize);
      
    assertTrue ( p != null
                  && p.getHeader().getTupleSize() == expectedSize );    
  }

  // Generate a set of pages filled with random tuples, and check
  // the tuples are correctly retrieved from the page with
  // getTuple at a specific offset.
  @Test
  public void getTest() {
    List<ContiguousPage> dataPages =
        ptUtils.generateContiguousPages(fillBackward, schema, tuples);

    ListIterator<Tuple> tupleIt = tuples.listIterator();

    for (ContiguousPage p : dataPages) {
      PageHeader hdr = p.getHeader();
      short offset =
          hdr.filledBackward()? hdr.getCapacity() : hdr.getHeaderSize();

      for (; hdr.filledBackward()?
              offset > hdr.getFreeSpaceOffset() :
              offset < hdr.getFreeSpaceOffset(); )
      {
        assertTrue ( tupleIt.hasNext() );
        Tuple check = tupleIt.next();

        if ( hdr.filledBackward() ) { offset -= check.size(); }

        assertTrue( p.isValidData(offset, (short) check.size()) );
        
        // Tuple retrieval
        Tuple t = p.getTuple(offset);
        assertTrue ( t != null );
        assertTrue ( t.equals(check) );
        
        if ( !hdr.filledBackward() ) { offset += check.size(); }
      }
    }

    // Ensure that all tuples generated have been verified.
    assertTrue ( !tupleIt.hasNext() );
  }
 
  // Tests whether a set of randomly generated tuples can be correctly
  // added to the page, based on the free space offsets before and
  // after the put operation.
  @Test
  public void putTest() {
    ContiguousPage p = null;
    for (Tuple t : tuples) {
      // Get a new page if the previous one is full.
      if ( p == null || p.getHeader().getFreeSpace() < t.size()) {
        p = ptUtils.getContiguousPage(schema, fillBackward);
      }

      assertTrue ( p != null && p.getHeader() != null );

      short tupleSize = (short) t.size();

      short fsBefore = p.getHeader().getFreeSpace();
      short fsOffsetBefore = p.getHeader().getFreeSpaceOffset();

      // Put the generated data in the page.
      boolean valid = p.putTuple(t, tupleSize);

      short fsDiff = (short) (fsBefore - p.getHeader().getFreeSpace());
      short fsOffsetDiff =
          (short) (p.getHeader().getFreeSpaceOffset() - fsOffsetBefore);

      assertTrue ( valid
                    && fsDiff == tupleSize
                    && (fillBackward?
                        (fsOffsetDiff == -tupleSize)
                        : fsOffsetDiff == tupleSize) );
    }    
  }
  
  // Similar to the putTuple test above, except with insertTuple.
  @Test
  public void insertTest() {
    List<ContiguousPage> dataPages =
        ptUtils.generateContiguousPages(fillBackward, schema, tuples);
    List<Tuple> newTuples = ptUtils.getTuples(schema, numTuples);
    ListIterator<Tuple> tupleIt = newTuples.listIterator();
    
    // For each page, insert a new tuple at the location of each existing tuple.
    for (ContiguousPage p : dataPages) {
      PageHeader hdr = p.getHeader();

      // An offset at which to insert, starting at one extreme of the page.
      // This will vary over all existing tuples.
      short insertOffset =
          hdr.filledBackward()? hdr.getCapacity() : hdr.getHeaderSize();

      // Get the extent of existing tuples.
      short boundary = hdr.getFreeSpaceOffset();

      for (; hdr.filledBackward()? insertOffset > boundary : insertOffset < boundary; )
      {
        // New tuples should have as many tuples as the original dataset.
        assertTrue ( tupleIt.hasNext() );
        Tuple insert = tupleIt.next();

        short tupleSize = (short) insert.size();

        if ( hdr.filledBackward() ) { insertOffset -= tupleSize; }

        assertTrue( p.isValidData(insertOffset, tupleSize) );

        short fsBefore = p.getHeader().getFreeSpace();
        short fsOffsetBefore = p.getHeader().getFreeSpaceOffset();
        
        boolean valid = p.insertTuple(insertOffset, insert);

        short fsDiff = (short) (fsBefore - p.getHeader().getFreeSpace());
        short fsOffsetDiff =
            (short) (p.getHeader().getFreeSpaceOffset() - fsOffsetBefore);

        assertTrue ( valid
                      && fsDiff == tupleSize
                      && (fillBackward?
                          (fsOffsetDiff == -tupleSize)
                          : fsOffsetDiff == tupleSize)
                      && p.getTuple(insertOffset).equals(insert) );

        if ( !hdr.filledBackward() ) { insertOffset += tupleSize; }
      }
      
      // Ensure that all new tuples have been inserted.
      assertTrue ( !tupleIt.hasNext() );
    }
  }

  @Test
  public void removeTest() {
    List<ContiguousPage> dataPages =
        ptUtils.generateContiguousPages(fillBackward, schema, tuples);
    
    for (ContiguousPage p : dataPages) {
      PageHeader hdr = p.getHeader();
      short tupleSize = hdr.getTupleSize();
      int pageRemovals = 0;
      
      // An offset at which to remove, starting at one extreme of the page.
      // This will vary over all existing tuples.
      short removeOffset =
          (short) (hdr.getFreeSpaceOffset()
                    - (hdr.filledBackward()? 0 : tupleSize));

      // Get the extent of existing tuples.
      short boundary = hdr.filledBackward()? hdr.getCapacity() : hdr.getHeaderSize();

      // Remove tuples in reverse order from the fill direction.
      for (; hdr.filledBackward()?
              removeOffset < boundary : removeOffset >= boundary; )
      {
        assertTrue( p.isValidData(removeOffset, tupleSize) );

        short fsBefore = p.getHeader().getFreeSpace();
        short fsOffsetBefore = p.getHeader().getFreeSpaceOffset();

        boolean valid = p.removeTuple(removeOffset);
        
        short fsDiff = (short) (p.getHeader().getFreeSpace() - fsBefore);
        short fsOffsetDiff =
            (short) (fsOffsetBefore - p.getHeader().getFreeSpaceOffset());

        assertTrue ( valid
                      && fsDiff == tupleSize
                      && (fillBackward?
                          (fsOffsetDiff == -tupleSize)
                          : fsOffsetDiff == tupleSize) );

        if ( hdr.filledBackward() ) { removeOffset += tupleSize; }
        else { removeOffset -= tupleSize; }

        pageRemovals += 1;
      }

      assertTrue( pageRemovals == tuples.size() );
    }
  }

  @Test
  public void clearTest() {
    List<ContiguousPage> dataPages =
        ptUtils.generateContiguousPages(fillBackward, schema, tuples);
    
    for (ContiguousPage p : dataPages) {
      PageHeader hdr = p.getHeader();
      short tupleSize = hdr.getTupleSize();

      assertTrue( hdr.getUsedSpace() == (tuples.size() * tupleSize) );
      
      p.clearTuples();
      
      assertTrue( hdr.getUsedSpace() == 0
                  && hdr.getFreeSpaceOffset() ==
                      (hdr.filledBackward()?
                          hdr.getCapacity() : hdr.getHeaderSize()) );
    }
  }
  
}
