package org.joy.io.persist;

import com.sleepycat.bind.EntityBinding;
import com.sleepycat.bind.EntryBinding;

public class EntitySetConfig {
	private EntityBinding eBinding;
	private EntryBinding keyBinding;
	private long bufferSize = 512 * 1024;

	public EntitySetConfig() {
		// TODO Auto-generated constructor stub
	}

	public EntitySetConfig(EntityBinding binding, EntryBinding keyBinding) {
		super();
		eBinding = binding;
		this.keyBinding = keyBinding;
	}

	public long getBufferSize() {
		return bufferSize;
	}

	public void setBufferSize(long size) {
		this.bufferSize = size;
	}

	public EntityBinding getEBinding() {
		return eBinding;
	}

	public EntryBinding getKeyBinding() {
		return keyBinding;
	}
}
