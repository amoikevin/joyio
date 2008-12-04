package org.joy.io.persist;


import org.joy.io.DBException;
import org.joy.io.DataSet;
import org.joy.io.Reader;
import org.joy.io.config.DataSetConfig;

import com.sleepycat.bind.ByteArrayBinding;
import com.sleepycat.bind.EntityBinding;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.db.CursorConfig;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.LockMode;
import com.sleepycat.persist.EntityCursor;

public class EntitySet<E> {

    private DataSet set;
    private EntityBinding eBinding;
    private EntryBinding keyBinding;

    public EntitySet(EntityCursor<E> c, EntitySetConfig config)
            throws DBException {
        // TODO Auto-generated constructor stub
        this.eBinding = config.getEBinding();
        this.keyBinding = config.getKeyBinding();

        DataSetConfig setConfig = new DataSetConfig();
        setConfig.setKeyBinding(new ByteArrayBinding());
        setConfig.setValBinding(new ByteArrayBinding());
        setConfig.setBufferSize(config.getBufferSize());
        set = new DataSet(setConfig);

        try {
            // 导入数据
            for (E e : c) {
                put(e);
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            throw new DBException();
        }
    }

    public void put(E e) throws DBException {
        DatabaseEntry keyE = new DatabaseEntry();
        eBinding.objectToKey(e, keyE);
        DatabaseEntry valE = new DatabaseEntry();
        eBinding.objectToData(e, valE);
        set.put(keyE.getData(), valE.getData());
    }

    public void delete(Object key) throws DBException {
        DatabaseEntry keyE = new DatabaseEntry();
        keyBinding.objectToEntry(key, keyE);
        set.remove(keyE.getData());
    }

    public boolean contains(Object key) throws DBException {
        DatabaseEntry keyE = new DatabaseEntry();
        keyBinding.objectToEntry(key, keyE);
        return set.contains(keyE.getData());
    }

    public void join(EntitySet<E> dataSet) throws DBException {
        DataSet s1 = dataSet.getSet();
        CursorConfig c = new CursorConfig();
        c.setWriteCursor(true);
        Reader r = set.getReader(c);
        while (r.next(LockMode.READ_UNCOMMITTED)) {
            if (!s1.contains(r.getKey())) //set.remove(r.getKey());
            {
                r.delete();
            }
        }
        r.close();
    }

    public E get(Object key) throws DBException {
        DatabaseEntry keyE = new DatabaseEntry();
        DatabaseEntry valE;
        keyBinding.objectToEntry(key, keyE);
        byte[] b = (byte[]) set.get(keyE.getData());
        if (b != null) {
            valE = new DatabaseEntry(b);
            return (E) eBinding.entryToObject(keyE, valE);
        } else {
            return null;
        }
    }

    public void close() throws DBException {
        set.close();
    }

    public DataSet getSet() {
        return set;
    }

    public EntityReader<E> getReader() throws DBException {
        return new EntityReader<E>(eBinding, keyBinding, set.getReader());
    }
}
