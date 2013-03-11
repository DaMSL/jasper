package edu.jhu.cs.damsl.catalog;

import java.util.List;
import edu.jhu.cs.damsl.catalog.identifiers.FileId;

public interface Durable extends Addressable {
  public List<FileId> files();
}
