package edu.jhu.cs.damsl.language.core.types;

import java.nio.charset.Charset;

import org.slf4j.LoggerFactory;

import edu.jhu.cs.damsl.catalog.Defaults;
import edu.jhu.cs.damsl.engine.storage.Tuple;

public class StringType extends Type {

  protected static boolean isUTF16 =
    Defaults.defaultCharset == Charset.forName("UTF-16");

  public StringType() {
    super(String.class);
  }

  // TODO: make more efficient for non-UTF16 charsets.
  public static int getStringByteLength(String s) {
    int r = -1;
    // String.length() returns the number of Java chars, i.e. 16-bit code units
    if ( isUTF16 ) r = (s.length()+1)*2;
    else {
      // TODO: this is inefficient since it allocates a new byte array just
      // to correctly determine its length.
      byte[] b = s.getBytes(Defaults.defaultCharset);
      r = b.length;
    }
    return r;
  }

  @Override
  public Integer getSize() { return -1; }

  @Override
  public Integer getInstanceSize(Object o) { 
    if ( o instanceof String ) {
      String s = (String) o;
      return (Integer.SIZE >> 3)+getStringByteLength(s);
    }
    return -1;
  }

  @Override
  public Object newValue() { return ""; }

  // TODO: this assumes that two Jasper processes have the same string
  // encoding. If they do not, this will result in additional unmapped chars.
  @Override
  public Object readValue(Tuple t) {
    int length = t.readInt();
    String r = t.readSlice(length).toString(Defaults.defaultCharset);
    return r;
  }

  // TODO: getBytes returns a new byte array. Is there any way to do this
  // without creating an internal copy of the string?
  @Override
  public void writeValue(Object v, Tuple t) {
    String s = (String) v;
    t.writeInt(getStringByteLength(s));
    t.writeBytes(s.getBytes(Defaults.defaultCharset));
  } 

  public Object parseType(String s) {
      return (Object)s;
  }

}
