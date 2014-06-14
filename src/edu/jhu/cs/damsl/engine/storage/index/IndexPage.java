package edu.jhu.cs.damsl.engine.storage.index;

import org.jboss.netty.buffer.ChannelBuffer;

import edu.jhu.cs.damsl.catalog.Defaults;
import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TupleId;
import edu.jhu.cs.damsl.catalog.identifiers.tuple.ContiguousTupleId;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.iterator.index.IndexEntryIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.index.IndexEntryPageIterator;
import edu.jhu.cs.damsl.engine.storage.page.ContiguousPage;
import edu.jhu.cs.damsl.factory.tuple.TupleIdFactory;
import edu.jhu.cs.damsl.utils.hw2.HW2.*;

/**
 *  A page implementation for B+ tree indexes.
 *  Index pages should contain a sequence of index entries, sorted by
 *  the index key. Lookup operations perform binary search through the key.
 */
@CS316Todo(methods = "getEntry, putEntry, removeEntry")
@CS416Todo(methods = "getEntry, putEntry, removeEntry")
public class IndexPage<IdType extends TupleId> extends ContiguousPage
{
  // Bit flag in the page header indicating whether this is a leaf node.
  public final static byte LEAF_NODE = 0x4;

  public final static double FILL_FACTOR = Defaults.defaultFillFactor;

  protected Schema schema;
  protected TupleIdFactory<IdType> factory;
  // A next page pointer.
  // For non-leaf pages, this is the right-most pointer for the page,
  // indicating the child page containing entries greater than the maximal
  // entry in this page.
  // For leaf pages, this is the next page in the leaf chain that can be
  // used to scan all tuples identified by the index.
  PageId nextPage;

  // Common constructors.
  public IndexPage(Integer id, ChannelBuffer buf, Schema sch, byte flags, boolean isLeaf) {
    super(id, buf, sch, (byte) (flags | (isLeaf? LEAF_NODE : 0)));
  }

  public IndexPage(PageId id, ChannelBuffer buf, Schema sch, byte flags, boolean isLeaf) {
    super(id, buf, sch, (byte) (flags | (isLeaf? LEAF_NODE : 0)));
  }

  // Factory constructors.
  // For index pages, 
  public IndexPage(Integer id, ChannelBuffer buf, Schema sch, byte flags) {
    this(id, buf, sch, flags, true);
  }

  public IndexPage(PageId id, ChannelBuffer buf, Schema sch, byte flags) {
    this(id, buf, sch, flags, true);
  }

  public IndexPage(Integer id, ChannelBuffer buf, Schema sch) {
    this(id, buf, sch, (byte) 0x0, true);
  }
  
  public IndexPage(PageId id, ChannelBuffer buf, Schema sch) {
    this(id, buf, sch, (byte) 0x0, true);
  }

  public IndexPage(Integer id, ChannelBuffer buf, byte flags) {
    this(id, buf, null, flags, true);
  }
  
  public IndexPage(PageId id, ChannelBuffer buf, byte flags) {
    this(id, buf, null, flags,  true);
  }

  public void setSchema(Schema sch){
	  this.schema = sch;
  }
  
  public Schema getSchema(){
	  return this.schema;
  }
  
  public void setTupleIdFactory(TupleIdFactory<IdType> factory){
	  this.factory = factory;
  }
  
  public boolean isLeaf() { return getHeader().isFlagSet(LEAF_NODE); }

  public PageId getNextPage() { return nextPage; }

  /* Index page API, to access index entries.
     Index entries are converted to and from tuples with the
     IndexEntry.read() and IndexEntry.write() methods. The corresponding
     tuples can then be written to and read from the ContiguousPage from
     which this IndexPage inherits.
  */

  // Retrieves the smallest index entry greater than the given key.
  // This should perform a binary search through the index entries on the page.
  @CS316Todo(exercise = 1)
  @CS416Todo(exercise = 1)
  
  //PROBLEM: if didn't find one, how to go to the next page.(needed for both while loop) How to say it stopped?
 
	public IndexEntry<IdType> getEntry(Tuple key) {
		ContiguousTupleId nexid = null;
		int size = key.getFixedLength();
		if (binarySearch(key) == -1) {
			return null;
		} else {
			int nextOffset = binarySearch(key) + size;
			nexid = new ContiguousTupleId(super.getId(), (short) size,
					(short) nextOffset);
			Tuple t = super.getTuple(nexid);
			IndexEntry<IdType> ie = new IndexEntry<IdType>(schema);
			ie.read(t, isLeaf(), factory);
			return ie;
		}
	}
  
  
  @FromMe
  private int binarySearch(Tuple key){
	    
	    int offset = 0;
		short headersize = super.getHeader().getHeaderSize();
		int size = key.getFixedLength();
		short capacity = super.getHeader().getCapacity();
		int max = super.getHeader().getUsedSpace() / size;
		int min = 0;
		int mid = max / 2;
		int flag=0;   // A flag to remember whether the tuple before is lareger or smaller than  the key tuple; 
		ContiguousTupleId ctid = null;
		
		while (true) {
			// get the mid tuple
			if (super.getHeader().filledBackward()) {
				offset = capacity-mid*size;
			} else {
				offset = headersize + mid * size;
			}
				ctid = new ContiguousTupleId(super.getId(), (short) size,
						(short) offset);
			Tuple t = super.getTuple(ctid);
			
		  // do binary search to get find tuple given the key
          // 1. base case. Return the next tuple.
			if (key.compareTo(t) == 0) {
		          return offset;		
			} else if (key.compareTo(t) < 0) {
				flag = 1;
				if(mid ==min){return offset;}
				mid = (mid + min) / 2;
			} else if (key.compareTo(t) > 0) {
				if(flag==1){return offset;} // There doesn't exist a equial tuple to the key, so return the first greater tuple
				if(mid ==max){return offset;} // There doesen't exist a greater tuple than key;
				mid = (max + mid) / 2;
			}
		}
  }
  @FromMe
	public IndexEntry<IdType> getEqualEntry(Tuple key) {
	  ContiguousTupleId nexid = null;
		int size = key.getFixedLength();
		if (binarySearch(key) == -1) {
			return null;
		} else {
			int nextOffset = binarySearch(key) ;
			nexid = new ContiguousTupleId(super.getId(), (short) size,
					(short) nextOffset);
			Tuple t = super.getTuple(nexid);
			IndexEntry<IdType> ie = new IndexEntry<IdType>(schema);
			ie.read(t, isLeaf(), factory);
			return ie;
		}
	}


	// Adds the given entry in sorted fashion to this index page.
	@CS316Todo(exercise = 2)
	@CS416Todo(exercise = 2)

	public boolean putEntry(IndexEntry<IdType> entry) {
		// //first find the index entry that is the right next to our input
	   Tuple t = entry.write();
	   int offset =0;
	   int size = super.getHeader().getTupleSize();
        offset = binarySearch(t)+size;
       ContiguousTupleId cid = new ContiguousTupleId(this.pageId,(short) size, (short) offset);
       return  super.insertTuple(cid, t);
       //return (cid == null);
  }

  // Removes the given from this index page.
  @CS316Todo(exercise = 3)
  @CS416Todo(exercise = 3)
  public boolean removeEntry(IndexEntry<IdType> entry) {
	   Tuple t = entry.write();
	   int size = super.getHeader().getTupleSize();
      int offset = binarySearch(t)+size;
      ContiguousTupleId cid = new ContiguousTupleId(this.pageId,(short) size, (short) offset);
      return  super.removeTuple(cid);
  }

  // An iterator over the entries in this page.
  public IndexEntryIterator<IdType> entry_iterator(
            Schema key, TupleIdFactory<IdType> factory)
  {
    return new IndexEntryPageIterator<IdType>(
                  getId(), this, key, factory);
  }

  public IndexEntryIterator<IdType> entry_iterator(
            Schema key, TupleIdFactory<IdType> factory,
            ContiguousTupleId start, ContiguousTupleId end)
  {
    return new IndexEntryPageIterator<IdType>(
                  getId(), this, key, factory, start, end);
  }
  
  public ContiguousTupleId getFirstTuple(){
	     short capacity = super.getHeader().getCapacity();
		short headersize = super.getHeader().getHeaderSize();
		short size = super.getHeader().getTupleSize();
		ContiguousTupleId ctid = null;
		if (super.getHeader().filledBackward()) {
		
			ctid = new ContiguousTupleId(super.getId(), (short) size,
					(short) (headersize + size));
		} else {
	
			ctid = new ContiguousTupleId(super.getId(), (short) size,
					(short) (capacity  - size));
		}
		// get the mid tuple
		//Tuple t = super.getTuple(ctid);
	  return ctid;
  }
  
	public ContiguousTupleId getLastTuple() {

		short capacity = super.getHeader().getCapacity();
		short headersize = super.getHeader().getHeaderSize();
		short size = super.getHeader().getTupleSize();
		int max = super.getHeader().getUsedSpace() / size;
		ContiguousTupleId ctid = null;
		if (super.getHeader().filledBackward()) {
			ctid = new ContiguousTupleId(super.getId(), (short) size,
					(short) (headersize + max * size));
		} else {
			ctid = new ContiguousTupleId(super.getId(), (short) size,
					(short) (capacity - max * size));
		}
		return ctid;
	}
}
