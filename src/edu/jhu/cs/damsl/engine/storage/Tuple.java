package edu.jhu.cs.damsl.engine.storage;

import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.buffer.DuplicatedChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.language.core.types.Type;

/**
 * Tuples, the basic unit of dataflow, implemented on top of Netty buffers.
 * 
 * Tuples can either be singletons or multi-tuples.
 * 
 * TODO: uniform implementation of singletons and multi-tuples. Singletons
 * should not use a heap-allocated header.
 * 
 * TODO: internal class for tuple headers.
 * 
 * @author Yanif Ahmad
 * @version 0.1
 */
public class Tuple extends DuplicatedChannelBuffer {
  
  private static final Logger logger = LoggerFactory.getLogger(Tuple.class); 
  
  // Header consists of only the tuple length for now.
  public static final Integer headerSize = 4;
  ChannelBuffer header;

  // Multi-tuples (i.e. tuple batches).
  // Multi-tuples contain multiple contiguous tuples. Each tuple has a header,
  // and a data segment. By supporting methods for multi-tuples, we avoid the
  // internal book-keeping overhead of constructing wrappers around each
  // individual tuple.
  
  // Number of tuples in this batch. -1 if unknown, which occurs when
  // constructing variable length tuples until each tuple is registered.
  private int tupleCount;

  // Size of a single tuple, including the header if fixed-length, -1 otherwise.
  private int tupleSize;
  
  // Absolute offset of the next tuple within the buffer.
  // Note this field is not maintained if there are direct writes to the
  // underlying buffer. 
  private int nextTupleOffset;
  
  // Private constructor, use static method below to create tuples.
  private Tuple(ChannelBuffer header, ChannelBuffer data,
                int tupleSize, int tupleCount)
  { 
    super(data);
    
    this.header = header;

    this.tupleSize = tupleSize;
    this.tupleCount = tupleCount;
    this.nextTupleOffset = (header == null? 0 : -1);
  }
  
  // Headers.

  // Returns the tuple length from a header contained in a separate buffer,
  // without modifying the header buffer's positions.
  private static int getSizeFromHeader(ChannelBuffer header) {
    if (header != null) return header.getInt(0);
    return 0;
  }

  // Returns tuple length from a header residing at an offset within
  // a channel buffer, without modifying the source buffer's positions.
  private static int getSizeFromHeader(ChannelBuffer src, int offset) {
    if (src != null) return src.getInt(offset);
    return 0;
  }

  // Header contained in this tuple's data buffer.
  private int readSizeFromHeader() { return readInt(); }

  // Constructs a heap-allocated header.
  public static ChannelBuffer getHeader(int tupleLength) {
    ChannelBuffer hdr = ChannelBuffers.buffer(4);
    hdr.writeInt(tupleLength);
    return hdr;
  }
  
  // Static constructors

  // Tuples for reading and copying.
  // These tuples have their writerIndexes set to the
  // end of the data segment, thus cannot be written to.
  
  // Creates a tuple as a view of the source ChannelBuffer.
  public static Tuple getTuple(ChannelBuffer source, int limitIndex) {
    Tuple r = null;
    int available = source.readableBytes();
    int nextPos = source.readerIndex()+headerSize;

    // Get tuple header if available
    if ( available > headerSize && nextPos <= limitIndex) {
      source.markReaderIndex();
      ChannelBuffer newHeader = source.readBytes(headerSize);
      int tupleSize = getSizeFromHeader(newHeader);
      nextPos += tupleSize; 
      // Initialize tuple with content if available, otherwise rewind.
      if ( (available - headerSize) >= tupleSize && nextPos <= limitIndex)
        r = new Tuple(newHeader, source.readBytes(tupleSize), -1, 1);
      else source.resetReaderIndex();
    }
    return r;
  }
    
  // Tuple size here should contain the header size.
  public static Tuple getTuple(ChannelBuffer source,
                               int tupleSize, int limitIndex)
  {
    Tuple r = null;
    int nextPos = source.readerIndex() + tupleSize;

    // Get tuple header and data if available
    if ( source.readableBytes() >= tupleSize && nextPos <= limitIndex ) {
      ChannelBuffer newHeader = source.readBytes(headerSize);
      r = new Tuple(newHeader, source.readBytes(tupleSize-headerSize), tupleSize, 1);
    }
    return r;    
  }
  
  public static Tuple getTuple(Tuple t) {
    Tuple r = null;
    if ( t.isMultiTuple() ) r = getTuple(t);
    else {
      int tupleSize = getSizeFromHeader(t.header);
      r = new Tuple(t.header, t.readBytes(tupleSize), tupleSize, 1);
      t.header = null; // Detach header from input tuple.
    }
    return r;
  }


  // Multi-tuple constructors
  
  public static Tuple getNTuples(ChannelBuffer source,
                                 int upperBound, int limitIndex)
  {
    Tuple r = null;
    int i = upperBound;
    int available = source.readableBytes();
    int nextPos = source.readerIndex()+headerSize;
    int consume = 0;
    
    while ( i > 0 && available > headerSize && nextPos <= limitIndex ) {
      int tupleSize = getSizeFromHeader(source, source.readerIndex()+consume);
      nextPos += tupleSize;
      if ( (available -= headerSize) >= tupleSize && nextPos <= limitIndex ) {
        consume += headerSize+tupleSize;
        available -= tupleSize;
        nextPos += headerSize;
        --i;
      } else break;
    }
    
    if ( consume > 0 )
      r = new Tuple(null, source.readBytes(consume), -1, upperBound-i);

    return r;
  }

  // Tuple size here should contain the header size.
  public static Tuple getNTuples(ChannelBuffer source,
                                 int tupleSize, int upperBound, int limitIndex)
  {
    Tuple r = null;
    int available = source.readableBytes();
    int m = available / tupleSize;
    int limit = (limitIndex - source.readerIndex())/tupleSize;
    int n = Math.min(Math.min(m, limit), upperBound);
    if ( n > 0 )
      r = new Tuple(null, source.readBytes(n*tupleSize), tupleSize, n);
    return r;
  }

  // Tuples for writing.

  /**
   * Creates a multi-tuple backed by the given buffer.
   * We explicitly set the writerIndex to match the buffer we're allocating
   * from, and advance the given buffer's writerIndex to ensure it does not
   * overwrite any operations on this tuple.
   * Notes: tupleSize here should include the header size. The result contains
   * a valid count only if the tupleSize is positive. Also, requestedSize 
   * should be integer multiples of tupleSize if the latter is positive.
   */
  public static Tuple emptyTuple(ChannelBuffer alloc,
                                 int requestedSize, int tupleSize)
  {
    int currentWritePos = alloc.writerIndex();
    ChannelBuffer dest = alloc.slice(currentWritePos, requestedSize);
    dest.writerIndex(dest.readerIndex());
    alloc.writerIndex(currentWritePos+requestedSize);
    int n = tupleSize > 0? requestedSize / tupleSize : -1;
    Tuple r = new Tuple(null, dest, tupleSize, n);
    return r;
  }
  
  /**
   * Creates a single heap-allocated tuple of the requested size, which should
   * NOT include the header.
   * Notes: the result is a single tuple with fixed length if indicated.
   */
  public static Tuple emptyTuple(int requestedSize, boolean varLength) {
    assert ( requestedSize > 0 );
    int sz = requestedSize+headerSize;
    ChannelBuffer dest = ChannelBuffers.buffer(sz);
    dest.writeInt(requestedSize);
    return new Tuple(dest.readBytes(headerSize), dest, varLength? -1 : sz, 1);
  }

  // Predefined tuples.

  /**
   * Creates a single heap-allocated tuple filled from the given fields.
   * Returns null if the schema does not return a valid tuple size for the
   * instance.
   */
  public static Tuple schemaTuple(Schema schema, List<Object> fields) {
    int size = schema.getTupleSize(fields);
    if ( size < 0 ) {
      logger.debug("invalid fields for schema {} {}", fields, schema);
      return null;
    }
    Tuple r = emptyTuple(size, schema.getTupleSize() < 0);
    return r.fillTuple(schema, fields)? r : null;
  }

  // Static skip methods.
  
  /**
   * Skips over the next tuple in the source buffer.
   * Performs an unchecked skip of the specified number of bytes.
   * Tuple size here should include the header size.
   * 
   * @param source the buffer containing a tuple to skip
   * @param tupleSize the size of the tuple
   * @return whether we successfully skipped a tuple
   */
  public static boolean skipTuple(ChannelBuffer source,
                                  int tupleSize, int limitIndex)
  {
    boolean r = false;
    if ( source.readableBytes() >= tupleSize
          && source.readerIndex()+tupleSize <= limitIndex )
    {
      source.skipBytes(tupleSize);
      r = true;
    }
    return r;
  }

  /**
   * Skips over the next tuple in the source buffer.
   * Retrieves the tuple header to get the payload length before skipping
   * over the remaining bytes.
   * 
   * @param source the buffer containing a tuple to skip
   * @return whether we successfully skipped a tuple
   */
  public static boolean skipTuple(ChannelBuffer source, int limitIndex) {
    boolean r = false;
    int available = source.readableBytes();
    int nextPos = source.readerIndex()+headerSize;

    // Get tuple header if available
    if ( available > headerSize && nextPos <= limitIndex ) {
      source.markReaderIndex();
      ChannelBuffer newHeader = source.readBytes(headerSize);
      int tupleSize = getSizeFromHeader(newHeader);
      nextPos += tupleSize;
      // Initialize tuple with content if available, otherwise rewind.
      if ( (available - headerSize) >= tupleSize && nextPos <= limitIndex ) {
        source.skipBytes(tupleSize);
        r = true;
      }
      else source.resetReaderIndex();
    }
    return r;
  }
  
  // Accessors

  public boolean isMultiTuple() { return header == null; }
  public boolean isFixedLength() { return tupleSize > 0; }
  public int getFixedLength() { return tupleSize; }

  // This method should be invoked every time we write to an allocated tuple
  // to track the batch size. This is only needed if buffer methods are being
  // invoked directly on the tuple.
  // We assume this method is invoked only after a complete tuple write (and
  // not in the middle of a partial write), that is, its writerIndex() is at
  // the end of the tuple.
  public boolean registerTuple(int bytesWritten) {
    boolean r = bytesWritten > 0;

    // Reset next offset.
    if ( r && nextTupleOffset < 0 ) {
      // Ensure we wrote a valid number of bytes for fixed size tuples.
      if ( tupleSize > 0 && bytesWritten == tupleSize)
        nextTupleOffset = writerIndex()-tupleSize;

      else if ( tupleSize < 0 )
        nextTupleOffset = writerIndex()-bytesWritten;
      
      else r = false;
    
      // Abort if invalid offset.
      if ( nextTupleOffset < readerIndex() ) nextTupleOffset = -1;
    }

    if ( r ) tupleCount = (tupleCount < 0? 1 : tupleCount+1);
    return r;
  }


  // Multi-tuple traversal.
  public boolean hasTuple() {
    return readerIndex() <= nextTupleOffset && nextTupleOffset < writerIndex();
  }


  // Advances a multi-tuple to the next data segment.
  // This method is only valid for read-only tuples, that is, tuples that are
  // not modified after construction.
  public void nextTuple() {
    if ( nextTupleOffset >= 0 ) {
      readerIndex(nextTupleOffset);
      if ( tupleSize > 0 ) {
        skipBytes(headerSize);
        nextTupleOffset += tupleSize;
      } else {
        int tupleSize = readSizeFromHeader();
        nextTupleOffset = readerIndex() + tupleSize;
      }
      
      // Abort if invalid offset.
      if ( nextTupleOffset > writerIndex() ) nextTupleOffset = -1;
    }
  }
  
  // Concatenates two tuples together, returning a new tuple.
  // This is a zero-copy operation based on concatenating buffers.
  public Tuple concatTuple(Tuple other) {
    // Weak schema check, based on size alone. 
    if ( tupleSize == other.tupleSize) {
      ChannelBuffer cons =
        ChannelBuffers.wrappedBuffer(toBuffer(), other.toBuffer());
      return new Tuple(null, cons, tupleSize, count() + other.count());
    }
    return null;
  }
  
  // Fills this tuple from the given schema and fields if there is sufficient
  // capacity, returning success status.
  public boolean fillTuple(Schema schema, List<Object> fields)
  {
    int writeStart = writerIndex();
    int available = capacity() - writeStart;
    if ( schema.getTupleSize(fields) <= available )
    {
      // Check # schema fields against objects provided.
      Map<String, Type> schemaFields = schema.getFieldsAndTypes();
      if ( schemaFields.size() == fields.size() ) 
      {
        ListIterator<Object> it = fields.listIterator();
        for (Map.Entry<String, Type> e : schemaFields.entrySet()) {
          // Write an object if there is one, otherwise reset the tuple and abort.
          if ( it.hasNext() ) { 
            Object f = it.next();
            logger.trace("filling tuple field {} {}", this, f);
            e.getValue().writeValue(f, this);
          }
          else { writerIndex(writeStart); break; }
        }
      } else {
        logger.error("mismatched schema and field sizes {} vs {}",
            schemaFields.size(), fields.size());
      }
    } else {
      logger.info("insufficient space {} vs {}",
          available, schema.getTupleSize(fields));
    }
    logger.trace("filled tuple {}", this, writeStart);
    return writerIndex() != writeStart;
  }

  // Interprets the tuple's buffer according to the given schema.
  public List<Object> interpretTuple(Schema schema)
  {
    // Basic size checks.
    if ( isFixedLength() && 
          schema.getTupleSize() != (getFixedLength()-headerSize) ) 
    {
      logger.error("interpretTuple invalid schema, expected length {} found {}",
          getFixedLength()-headerSize, schema.getTupleSize());
      return null;
    }
    
    // Give up if there's no more data to be read from this batch.
    if ( isMultiTuple() && !hasTuple() ) { return null; }
    
    logger.trace("reading schema {} from {}", schema, this);

    // Read past the header if necessary.
    if ( isMultiTuple() ) nextTuple();

    // Interpret according to schema types.
    LinkedList<Object> r = new LinkedList<Object>();
    List<Type> fieldTypes = schema.getTypes();
    for (Type t : fieldTypes) {
      Object f = t.readValue(this);
      if (f != null) r.add(f);
    }
    
    logger.trace("interpreted {} for {}", r, schema);
    return ( r.size() == fieldTypes.size()? r : null );
  }
  
  // Returns a channel buffer representing the header and data segments.
  public ChannelBuffer toBuffer() {
    if ( isMultiTuple() ) return ChannelBuffers.wrappedBuffer(this);
    return ChannelBuffers.wrappedBuffer(header, this);
  }

  // Returns the amount of data readable from the buffer, including the header.
  public int size() {
    return (header == null? 0 : header.readableBytes())+readableBytes();
  }

  // Returns the number of tuples contained, -1 if unknown (i.e. when an 
  // allocated multi-tuple does not have any additions registered)
  public int count() { return isMultiTuple()? tupleCount : 1; }

  public String toString(Schema schema) {
    List<Object> fields = interpretTuple(schema);
    if ( fields == null ) {
      logger.warn("failed to interpret {} with schema {}", this, schema);
      return null;
    }

    StringBuffer b = new StringBuffer();
    for (Object f : fields) { b.append(b.length() == 0? f : (","+f)); }
    return b.toString();
  }
  
}
