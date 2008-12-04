package org.joy.io.persist;

import org.joy.io.DBException;
import org.joy.io.Reader;

import com.sleepycat.bind.EntityBinding;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.db.DatabaseEntry;

public class SequentialReader<E> {
	private EntityBinding eBinding;
	private EntryBinding keyBinding;
	private Reader origin;

	public SequentialReader(EntityBinding binding, EntryBinding keyBinding,
			Reader orginReader) {
		super();
		eBinding = binding;
		this.keyBinding = keyBinding;
		this.origin = orginReader;
	}

	public boolean next() throws DBException{
		return origin.next();
	}
	
	public E getEntity(){
		DatabaseEntry keyE = new DatabaseEntry((byte[])origin.getKey());
		DatabaseEntry valE = new DatabaseEntry((byte[])origin.getValue());
		
		return (E) eBinding.entryToObject(keyE, valE);
	}
	
	public void close() throws DBException{
		origin.close();
	}
}
