package edu.jhu.cs.damsl.engine.storage.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.IndexId;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TupleId;
import edu.jhu.cs.damsl.catalog.identifiers.tuple.ContiguousTupleId;
import edu.jhu.cs.damsl.engine.storage.DbBufferPool;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.iterator.index.IndexEntryIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.index.IndexIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.page.ContiguousPageIterator;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.engine.transactions.TransactionAbortException;
import edu.jhu.cs.damsl.utils.hw2.HW2.CS316Todo;
import edu.jhu.cs.damsl.utils.hw2.HW2.CS416Todo;

/**
 * Represents the logical interface of an index.
 */
@CS316Todo(methods = "isEmpty, containsEntry, containsKey, firstKey, lastKey, " +
                     "lowerBound, upperBound, get, getAll, put, putAll, remove, removeAll")

@CS416Todo(methods = "isEmpty, containsEntry, containsKey, firstKey, lastKey, " +
                     "lowerBound, upperBound, get, getAll, put, putAll, remove, removeAll")

public class Index<IdType extends TupleId>
{
  IndexId indexId;
  IndexFile<IdType> indexFile;

  DbBufferPool<IndexId, ContiguousTupleId,
               PageHeader, IndexPage<IdType>, IndexFile<IdType>> pool;

  public Index(DbBufferPool<IndexId, ContiguousTupleId, PageHeader,
                            IndexPage<IdType>, IndexFile<IdType>> p,
               IndexId id, IndexFile<IdType> f)
  {
    pool = p;
    indexId = id;
    indexFile = f;
  }

  public IndexId getId() { return indexId; }


  /**
   * Return the file backing this index.
   */
  public IndexFile<IdType> getFile() { return indexFile; }


  // Index API, based on a multimap.
  
  /**
   * Return whether or not the index is empty.
   */
  @CS316Todo(exercise = 1)
  @CS416Todo(exercise = 1)
  public boolean isEmpty() {
    IndexFile<IdType> file = this.getFile();
    int numPages = file.numPages();
    if(numPages == 0)
    	{return false;}
    else {return true;}
  }
  
  /**
   * Determine if the index contains the given (key, record) association.
   */
  @CS316Todo(exercise = 1)
  @CS416Todo(exercise = 1)
  public boolean containsEntry(Tuple key, IdType record) {
  PageId root = indexFile.getRoot();
  IndexPage<IdType> page = pool.getPage(root);
 while(true){
	 IndexEntry<IdType> ie = page.getEqualEntry(key);
	 if((ie==null)&&(page.isLeaf())){
		 return false;
	 }
	 else if(ie==null){
		 page = pool.getPage(page.nextPage);
	 }
	 else if(ie.tupleId.equals(record)){
		 return true;
	 }
 }
  }
  
  /**
   * Determine if the given key is contained in the index.
   */
  @CS316Todo(exercise = 1)
  @CS416Todo(exercise = 1)
  public boolean containsKey(Tuple key) {
	  PageId root = indexFile.getRoot();
	  IndexPage<IdType> page = pool.getPage(root);
	 while(true){
		 IndexEntry<IdType> ie = page.getEqualEntry(key);
		 if((ie==null)&&(page.isLeaf())){
			 return false;
		 }
		 else if(ie==null){
			 page = pool.getPage(page.nextPage);
		 }
		 else {return true;}
	 }
  }
    
  /**
   * Get the first key (in sorted order) in the index.
   */
  @CS316Todo(exercise = 1)
  @CS416Todo(exercise = 1)
//Note from me: the idea is to create a method in page to get the first tuple. 
//If the page is a leaf page, the first tuple will be result;
//If not, we do the same thing with the page that pointed from this first tuple.
  public Tuple firstKey() {
		PageId pid = this.getFile().getRoot();
		IndexPage<IdType> p = this.pool.getPage(pid);
		Schema sch = p.schema;
		while (true) {
			if (p.isLeaf()) {
				Tuple t = p.getTuple(p.getFirstTuple());
				return t;
			} else {
				Tuple t = p.getTuple(p.getFirstTuple());
				IndexEntry<IdType> ie = new IndexEntry<IdType> (sch);
			    ie.read(t, false, this.indexFile.factory);
				PageId npid = ie.child();
				p = this.pool.getPage(npid);
			}
		}
	}
    
  /**
   * Return the last key (in sorted order) in the index.
   */
  @CS316Todo(exercise = 1)
  @CS416Todo(exercise = 1)
  public Tuple lastKey() {
	  PageId pid = this.getFile().getRoot();
	  IndexPage<IdType> p = this.pool.getPage(pid);
	//	Schema sch = p.getSchema();
		while (true) {
			if (p.isLeaf()) {
				Tuple t = p.getTuple(p.getLastTuple());
				return t;
			} else {
				PageId npid = p.nextPage;
				p = this.pool.getPage(npid);
				}
		}
 }

  /** Returns the first leaf entry that matches the given key.
    */
  @CS316Todo(exercise = 1)
  @CS416Todo(exercise = 1)
  protected ContiguousTupleId lowerBound(Tuple key) {
    
	    PageId  pid = indexFile.getRoot();
		IndexPage<IdType> page = pool.getPage(pid);
		ContiguousPageIterator it = page.iterator();
		ContiguousTupleId ctid = null;
		Tuple t = null;
		short size = page.getHeader().getTupleSize();
		short capacity = page.getHeader().getCapacity();
		short headersize = page.getHeader().getHeaderSize();
		int i = 0; //counter to record which tuple in the page matches

		if(page.isLeaf()){
			while(it.hasNext()){
				t = it.next();
				i++;
				if(t.equals(key)){
					if (page.getHeader().filledBackward()){
						ctid = new ContiguousTupleId(pid, size, (short) (capacity - size * i));
						return ctid;
					}
					else{
						ctid = new ContiguousTupleId(pid, size, (short) (headersize + size * i));
					}
				}
			}
			return null;
		}
		else{
			while(it.hasNext()){
				t = it.next();
				i++;
				if(t.equals(key)){
					if (page.getHeader().filledBackward()){
						ctid = new ContiguousTupleId(pid, size, (short) (capacity - size * i));
						return ctid;
					}
					else{
						ctid = new ContiguousTupleId(pid, size, (short) (headersize + size * i));
					}
				}
			}
			page = pool.getPage(page.nextPage);
			i=0;
		}
		return null;
	}

  /** Returns the first leaf entry that is strictly greater
    * than the given key.
    */ 
  @CS316Todo(exercise = 1)
  @CS416Todo(exercise = 1)
  protected ContiguousTupleId upperBound(Tuple key) {
	  PageId  pid = indexFile.getRoot();
		IndexPage<IdType> page = pool.getPage(pid);
		ContiguousPageIterator it = page.iterator();
		ContiguousTupleId ctid = null;
		Tuple t = null;
		short size = page.getHeader().getTupleSize();
		short capacity = page.getHeader().getCapacity();
		short headersize = page.getHeader().getHeaderSize();
		int i = 0; //counter to record which tuple in the page matches

		if(page.isLeaf()){
			while(it.hasNext()){
				t = it.next();
				i++;
				if(t.compareTo(key)>0){
					if (page.getHeader().filledBackward()){
						ctid = new ContiguousTupleId(pid, size, (short) (capacity - size * i));
						return ctid;
					}
					else{
						ctid = new ContiguousTupleId(pid, size, (short) (headersize + size * i));
					}
				}
			}
			return null;
		}
		else{
			while(it.hasNext()){
				t = it.next();
				i++;
				if(t.equals(key)){
					if (page.getHeader().filledBackward()){
						ctid = new ContiguousTupleId(pid, size, (short) (capacity - size * i));
						return ctid;
					}
					else{
						ctid = new ContiguousTupleId(pid, size, (short) (headersize + size * i));
					}
				}
			}
			page = pool.getPage(page.nextPage);
			i=0;
		}
		return null;
  }

  /**
   * Get the first tuple corresponding to the given key
   * in the index, null if no such tuple exists.
   */
  @CS316Todo(exercise = 1)
  @CS416Todo(exercise = 1)

	public IdType get(Tuple key) {
		PageId rootid = this.indexFile.getRoot();
		IndexPage<IdType> root = this.pool.getPage(rootid);
		IndexEntry<IdType> ie = root.getEntry(key);
		while (true) {
			if (ie == null) {
				IndexPage<IdType> p = this.pool.getPage(root.getNextPage());
				ie = p.getEntry(key);
				if ((p.isLeaf() == true) && (ie == null)) {
					return null;
				}
				return ie.tuple();
				
			}
		}
	}

  /**
   * Get all the tuples corresponding to the given key
   * in the index, an empty collection if no such tuple exists.
   */
  @CS316Todo(exercise = 1)
  @CS416Todo(exercise = 1)
  public Collection<IdType> getAll(Tuple key) {
	 ArrayList<IdType> tupleList = new ArrayList<IdType>();
	  IndexIterator<IdType> it = indexFile.index_iterator();
	  while(it.hasNext()){
		  tupleList.add(this.get(key));
	  }
	  return tupleList;
  }
    
  /**
   * Insert the given key into the index, pointing to the given
   * tuple in the base relation.
   */
  @CS316Todo(exercise = 2)
  @CS416Todo(exercise = 2)
	public boolean put(Tuple key, IdType id) {
         PageId root = indexFile.getRoot();
         IndexPage<IdType> page = pool.getPage(root);
         IndexEntry<IdType> child = null;
         this.insert(page, key,id, child);
         return (child==null);
  }
  
    private IndexEntry<IdType> insert(IndexPage<IdType> page, Tuple key, IdType id, IndexEntry<IdType> child){

		// Basic information to return result
		Schema schema = page.schema;
		IndexEntry<IdType> key_entry = new IndexEntry<IdType>(schema);
		key_entry.read(key, true, this.indexFile.factory);
		short size = page.getHeader().getTupleSize();
		int capacity = page.getHeader().getCapacity();
		int maxTupleNum = page.getHeader().getCapacity() / size;
		double leastTupleNum = IndexPage.FILL_FACTOR * maxTupleNum;
	//	PageId next = null;

		// variable for recurse
		// PageId child = null;
		// IndexPage<IdType> page = null;
		// PageId pid = null;
		// page = pool.getPage(pid);

		// First, find the right page to insert in;
			if (!page.isLeaf()) {
				IndexEntry<IdType> ie = page.getEntry(key);
				if (ie == null) {
					PageId pid = page.getNextPage();
					page=pool.getPage(pid);
					insert(page,key, id, child);	
				}
				else{
				PageId	pid=ie.pageId;
				page=pool.getPage(pid);
				insert(page,key, id, child);	
				}
				 if(child == null) {return child;}
				 else {
					 if (page.getHeader().isSpaceAvailable(size)) {
		                 page.putEntry(child);
						 child = null;
						 return child;
					 }
					 else{
						    // 1 find the split point
							// Tuple mid = page.getMid();
							PageId currentId = page.getId();
							int curr_pid = currentId.pageNum();
							// 2 create a new page
							PageId splitId = pool.getWriteablePage(indexId, (short)capacity);
							IndexPage<IdType> splitPage = pool.getPage(splitId);

							// 3 put the tuples in
							int offset = 0;
							// ContiguousTupleId to_ctid= null;
							ContiguousTupleId from_ctid = null;
							if (splitPage.getHeader().filledBackward()) {
								offset = 0;
								for (int i = 0; (curr_pid - i) >= leastTupleNum; i++) {
									from_ctid = new ContiguousTupleId(splitId,
											(short) size, (short) (capacity - size * i));
									Tuple t = page.getTuple(from_ctid);
									page.removeTuple(from_ctid);
									splitPage.putTuple(t);
								}
							} else {
								offset = splitPage.getHeader().getHeaderSize();
								for (int i = 0;(curr_pid - i) >= leastTupleNum; i++) {
									from_ctid = new ContiguousTupleId(splitId,
											(short) size, (short) (offset + size * i));
									Tuple t = page.getTuple(from_ctid);
									page.removeTuple(from_ctid);
									splitPage.putTuple(t);
								}
							}
							// 4 adjust nextPage pointer
							splitPage.nextPage = page.getNextPage();
							page.nextPage = splitPage.getId();
        
					     if(page.equals(this.pool.getPage( indexFile.getRoot() ))){
					    		PageId newRootId = pool.getWriteablePage(indexId, (short)capacity);
								IndexPage<IdType> newRoot = pool.getPage(newRootId);
								Tuple oldRoot = page.getTuple(page.getLastTuple());
					    	    IndexEntry<IdType> root_ie = new IndexEntry<IdType> (schema);
					    	    ie.read(oldRoot, false, this.indexFile.factory);
					    	    newRoot.putEntry(root_ie);
					    	    newRoot.nextPage = splitId;
					    	    indexFile.rootId = newRootId;
					     }
							//6 set the child pointer
							child = null;
							return child;
					 }
				 }
			}
			//reach to a leaf page
			else{
				// The leaf page has enough space
				if (page.getHeader().isSpaceAvailable(size)) {
					page.putEntry(key_entry);
					child = null;
					 return child;
				} 
				//don't have enough space
				else{
					// Need to split the lead page;
					// 1 find the split point
					// Tuple mid = page.getMid();
					PageId currentId = page.getId();
					int curr_pid = currentId.pageNum();
					// 2 create a new page
					PageId splitId = pool.getWriteablePage(indexId, (short)capacity);
					IndexPage<IdType> splitPage = pool.getPage(splitId);

					// 3 put the tuples in
					int offset = 0;
					// ContiguousTupleId to_ctid= null;
					ContiguousTupleId from_ctid = null;
					if (splitPage.getHeader().filledBackward()) {
						offset = 0;
						for (int i = curr_pid + 1; i <= maxTupleNum; i++) {
							from_ctid = new ContiguousTupleId(splitId,
									(short) size, (short) (capacity - size * i));
							Tuple t = page.getTuple(from_ctid);
							page.removeTuple(from_ctid);
							splitPage.putTuple(t);
						}
					} else {
						offset = splitPage.getHeader().getHeaderSize();
						for (int i = curr_pid + 1; i <= maxTupleNum; i++) {
							from_ctid = new ContiguousTupleId(splitId,
									(short) size, (short) (offset + size * i));
							Tuple t = page.getTuple(from_ctid);
							page.removeTuple(from_ctid);
							splitPage.putTuple(t);
						}
					}
					// 4 adjust nextPage pointer
					splitPage.nextPage = page.getNextPage();
					page.nextPage = splitPage.getId();

					// 5 set the child pointer
					Tuple temp = splitPage.getTuple(splitPage.getFirstTuple());
					child = new IndexEntry<IdType> (schema);
					child.read(temp, true, indexFile.factory);
					return child;
				}
			}
	}
  /**
   * Insert the given key into the index, once for each
   * occurrence in the base relation.
   */
  @CS316Todo(exercise = 2)
  @CS416Todo(exercise = 2)
  public boolean putAll(Tuple key, List<IdType> ids) {
	  IndexIterator<IdType> it = indexFile.index_iterator();
	  while(it.hasNext()){
		  this.put(key, it.next());
	  }
	  return true;
  }
    
  /**
   * Remove a particular (key, occurrence) from the index.
   */
  @CS316Todo(exercise = 3)
  @CS416Todo(exercise = 3)
  public boolean remove(Tuple key, IdType id) {
	  PageId root = indexFile.getRoot();
      IndexPage<IdType> page = pool.getPage(root);
      IndexEntry<IdType> child = null;
      IndexPage<IdType> parent = null;
      this.delete(page, key, id, child, parent);
      return (child==null);
  }

	private IndexEntry<IdType> delete(IndexPage<IdType> page, Tuple key,
			IdType id, IndexEntry<IdType> child, IndexPage<IdType> parent) {
		// Basic information to return result
		Schema schema = page.schema;
		IndexEntry<IdType> key_entry = new IndexEntry<IdType>(schema);
//		IndexEntry<IdType> sib_entry = new IndexEntry<IdType>(schema);
		IndexPage<IdType> sibPage = null;
		key_entry.read(key, true, this.indexFile.factory);
		short size = page.getHeader().getTupleSize();
		int capacity = page.getHeader().getCapacity();
		int maxTupleNum = page.getHeader().getCapacity() / size;
		double leastTupleNum = IndexPage.FILL_FACTOR * maxTupleNum;
		int curr_tupleNum = -1;
		int sib_tupleNum = -1;
		int parent_tupleNum = -1;

		int offset = -1; // offset of the split tuple
		int flag = -1; // indicate which page is on the right, -1: page is on
						// right -2 :sibling page on right

		if (!page.isLeaf()) {
			IndexEntry<IdType> ie = page.getEntry(key);
			if (ie == null) {
				PageId pid = page.getNextPage();
				parent = page;
				page = pool.getPage(pid);
				this.delete(page, key, id, child, parent);
			} else {
				PageId pid = ie.pageId;
				parent = page;
				page = pool.getPage(pid);
				this.delete(page, key, id, child, parent);
			}
			if (child == null) {
				return child;
			} else {
				page.removeEntry(child);
				// get a sibling page
				// if the page is not the first page belong to the parent, then
				// its sibling page is on the right,
				// which can get by using the nextPage
				if (!parent.getEntry(parent.getTuple(parent.getLastTuple())).pageId
						.equals(page)) {
					sibPage = this.pool.getPage(page.getNextPage());
					flag = -2;
				} else {
					if (parent.getHeader().filledBackward()) {
						offset = (parent.getLastTuple().offset() - size);
					} else {
						offset = (parent.getLastTuple().offset() + size);
					}
					Tuple tempt = parent.getTuple(new ContiguousTupleId(parent
							.getId(), size, (short) offset));
					IndexEntry<IdType> tempie = parent.getEntry(tempt);
					sibPage = this.pool.getPage(tempie.pageId);
					flag = -1;
				}
				// count how many tuples in sibling page and current page
				curr_tupleNum = page.getHeader().getUsedSpace() / size;
				sib_tupleNum = sibPage.getHeader().getUsedSpace() / size;
				parent_tupleNum = parent.getHeader().getUsedSpace() / size;

				// This page has enough entries to spare
				if (curr_tupleNum >= leastTupleNum) {
					child = null;
					return child;
				}
				// need to redistribution or merge
				else {
					// redistribution
					if ((curr_tupleNum + sib_tupleNum) > maxTupleNum) {
						ContiguousTupleId from_ctid = null;
				//		ContiguousTupleId pare_ctid = null;
						for (int j = 0; j <= (curr_tupleNum + sib_tupleNum - maxTupleNum); j++) {
							if (sibPage.getHeader().filledBackward()) {
								from_ctid = new ContiguousTupleId(
										sibPage.getId(), (short) size,
										(short) (capacity - size * j));
							} else {
								from_ctid = new ContiguousTupleId(
										sibPage.getId(), (short) size,
										(short) (offset + size * j));
							}
							Tuple t = sibPage.getTuple(from_ctid);
							IndexEntry<IdType> sie = new IndexEntry<IdType>(
									schema);
							sie.read(t, false, indexFile.factory);
							sibPage.removeEntry(sie);
							parent.putEntry(sie);
							if (parent.getHeader().filledBackward()) {
								from_ctid = new ContiguousTupleId(
										sibPage.getId(), (short) size,
										(short) (capacity - size * j));
							} else {
								from_ctid = new ContiguousTupleId(
										sibPage.getId(), (short) size,
										(short) (offset + size * j));
							}
						//	Tuple k = parent.getTuple(from_ctid);
							IndexEntry<IdType> pie = new IndexEntry<IdType>(
									schema);
							pie.read(t, false, indexFile.factory);
							parent.removeEntry(sie);
							page.putEntry(sie);
						}
						child = null;
						return child;
					}
					// merge
					else {
						IndexEntryIterator<IdType> it = parent.entry_iterator(
								schema, indexFile.factory);
						// sibling page on the right
						if (flag == -2) {
							while (it.hasNext()) {
								IndexEntry<IdType> temp = it.next();
								if (temp.child() == page.getId()) {
									child = temp;
								}
							}
							page.putEntry(child);
							parent.removeEntry(child);
							IndexEntryIterator<IdType> i = sibPage
									.entry_iterator(schema, indexFile.factory);
							while (i.hasNext()) {
								IndexEntry<IdType> j = i.next();
								sibPage.removeEntry(j);
								page.putEntry(j);
							}
							pool.discardPage(sibPage.getId());
						}
						// current page on the right
						else {
							while (it.hasNext()) {
								IndexEntry<IdType> temp = it.next();
								if (temp.child() == sibPage.getId()) {
									child = temp;
								}
							}
							page.putEntry(child);
							parent.removeEntry(child);
							IndexEntryIterator<IdType> i = page.entry_iterator(
									schema, indexFile.factory);
							while (i.hasNext()) {
								IndexEntry<IdType> j = i.next();
								page.removeEntry(j);
								sibPage.putEntry(j);
							}
							pool.discardPage(page.getId());
						}
						return child;
					}
				}
			}
		}
		// reach to a leaf page
		else {
			// delete the entry first
			page.removeEntry(key_entry);
			if (curr_tupleNum >= leastTupleNum) {
				child = null;
				return child;
			} else {
				// get a sibling page
				if (!parent.getEntry(parent.getTuple(parent.getLastTuple())).pageId
						.equals(page)) {
					sibPage = this.pool.getPage(page.getNextPage());
					flag = -2;
				} else {
					if (parent.getHeader().filledBackward()) {
						offset = (parent.getLastTuple().offset() - size);
					} else {
						offset = (parent.getLastTuple().offset() + size);
					}
					Tuple tempt = parent.getTuple(new ContiguousTupleId(parent
							.getId(), size, (short) offset));
					IndexEntry<IdType> tempie = parent.getEntry(tempt);
					sibPage = this.pool.getPage(tempie.pageId);
					flag = -1;
				}
				if ((curr_tupleNum + sib_tupleNum) > maxTupleNum) {
					ContiguousTupleId from_ctid = null;
				//	ContiguousTupleId pare_ctid = null;
					for (int j = 0; j <= (curr_tupleNum + sib_tupleNum - maxTupleNum); j++) {
						if (sibPage.getHeader().filledBackward()) {
							from_ctid = new ContiguousTupleId(sibPage.getId(),
									(short) size, (short) (capacity - size * j));
						} else {
							from_ctid = new ContiguousTupleId(sibPage.getId(),
									(short) size, (short) (offset + size * j));
						}
						Tuple t = sibPage.getTuple(from_ctid);
						IndexEntry<IdType> sie = new IndexEntry<IdType>(schema);
						sie.read(t, false, indexFile.factory);
						sibPage.removeEntry(sie);
						page.putEntry(sie);
					}
					IndexEntryIterator<IdType> it = parent.entry_iterator(
							schema, indexFile.factory);
					if (flag == -2) {
						while (it.hasNext()) {
							IndexEntry<IdType> temp = it.next();
							if (temp.child() == page.getId()) {
								child = temp;
								child.key = page.getTuple(page.getFirstTuple());
								child = null;
								return child;
							}
						}
					}
					// current page on the right
					else {
						while (it.hasNext()) {
							IndexEntry<IdType> temp = it.next();
							if (temp.child() == sibPage.getId()) {
								child = temp;
								child.key = page.getTuple(sibPage
										.getFirstTuple());
								child = null;
								return child;
							}
						}
					}
				}
				// merge on leaf node
				else {
					IndexEntryIterator<IdType> it = parent.entry_iterator(
							schema, indexFile.factory);
					if (flag == -2) {
						while (it.hasNext()) {
							IndexEntry<IdType> temp = it.next();
							if (temp.child() == page.getId()) {
								child = temp;
							}
						}
						IndexEntryIterator<IdType> i = sibPage.entry_iterator(
								schema, indexFile.factory);
						while (i.hasNext()) {
							IndexEntry<IdType> j = i.next();
							sibPage.removeEntry(j);
							page.putEntry(j);
						}
						// @@@@@adjust pointer to sibPage
						page.nextPage = sibPage.nextPage;
						pool.discardPage(sibPage.getId());
						return child;
					}
					// current page on the right
					else {
						while (it.hasNext()) {
							IndexEntry<IdType> temp = it.next();
							if (temp.child() == sibPage.getId()) {
								child = temp;
							}
						}
						IndexEntryIterator<IdType> i = page.entry_iterator(
								schema, indexFile.factory);
						while (i.hasNext()) {
							IndexEntry<IdType> j = i.next();
							page.removeEntry(j);
							sibPage.putEntry(j);
						}
						// @@@@@adjust pointer to sibPage
						sibPage.nextPage = page.nextPage;
						pool.discardPage(page.getId());
						return child;

					}
				}
			}
		}
		return child;
	}

	/**
   * Remove all occurrences of the given key from the index.
   */
  @CS316Todo(exercise = 3)
  @CS416Todo(exercise = 3)
  public boolean removeAll(Tuple key) {
	  IndexIterator<IdType> it = indexFile.index_iterator();
	  while(it.hasNext()){
		  this.remove(key, it.next());
	  }
	  return true;
  }

  
  // Iterators.
  
  /**
   * Return an iterator to the sorted contents of the base
   * relation, using the index.
   */
  public IndexIterator<IdType> scan() {
    return indexFile.index_iterator();
  }

  /**
   * Return an iterator to the sorted contents of the base
   * relation starting from the given key, using the index.
   */
  public IndexIterator<IdType> seek(Tuple key)
    throws TransactionAbortException
  {
    ContiguousTupleId start = lowerBound(key);
    ContiguousTupleId end = upperBound(key);
    return indexFile.index_iterator(start, end);
  }

}
