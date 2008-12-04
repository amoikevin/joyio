/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.joy.io.persist;

import com.sleepycat.bind.EntityBinding;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.DeadlockException;
import com.sleepycat.persist.EntityCursor;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.joy.io.DBException;
import org.joy.io.Reader;

/**
 * 
 * @author Lamfeeling
 */
public class EntityReader<E> {

    private EntityBinding eBinding;
    private EntryBinding keyBinding;
    private Reader origin;
    private EntityCursor<E> entityCursor;
    private E e;

    public EntityReader(EntityCursor<E> c) {
        entityCursor = c;
    }

    public EntityReader(EntityBinding binding, EntryBinding keyBinding, Reader reader) {
        this.eBinding = binding;
        this.keyBinding = keyBinding;
        this.origin = reader;
    }

    public void close() throws DBException {
        if (entityCursor != null) {
            try {
                entityCursor.close();
            } catch (DatabaseException ex) {
                Logger.getLogger(EntityReader.class.getName()).log(Level.SEVERE,
                        null, ex);
                throw new DBException();
            }
        } else {
            origin.close();
        }
    }

    public E getEntity() {
        if (entityCursor != null) {
            return e;
        } else {
            DatabaseEntry keyE = new DatabaseEntry((byte[]) origin.getKey());
            DatabaseEntry valE = new DatabaseEntry((byte[]) origin.getValue());

            return (E) eBinding.entryToObject(keyE, valE);
        }
    }

    public void delete() throws DBException {
        if (entityCursor != null) {
            try {
                entityCursor.delete();
            } catch (DatabaseException ex) {
                Logger.getLogger(EntityReader.class.getName()).log(Level.SEVERE,
                        null, ex);
                throw new DBException();
            }
        } else {
            origin.delete();
        }
    }

    void put(E e) throws DBException {
        if (entityCursor != null) {
            try {
                entityCursor.update(e);
            } catch (DatabaseException ex) {
                Logger.getLogger(EntityReader.class.getName()).log(Level.SEVERE,
                        null, ex);
                throw new DBException();
            }
        } else {
            DatabaseEntry keyE = new DatabaseEntry();
            eBinding.objectToKey(e, keyE);
            DatabaseEntry valE = new DatabaseEntry();
            eBinding.objectToData(e, valE);
            origin.put(keyE.getData(), valE.getData());
        }
    }

    /**
     * 把游标移动到下一个数据
     *
     * @return 是否存在下一个数据
     * @throws DBException
     */
    public boolean next() throws DBException {
        if (entityCursor == null) {
            return origin.next();
        }
        while (true) {
            try {
                e = entityCursor.next();
                break;
            } catch (DeadlockException ex) {
                System.err.println("死锁！！！！！");
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ex1) {
                    Logger.getLogger(EntityReader.class.getName()).log(
                            Level.SEVERE, null, ex1);
                }
            } catch (DatabaseException ex) {
                Logger.getLogger(EntityReader.class.getName()).log(
                        Level.SEVERE, null, ex);
                throw new DBException();
            }
        }
        if (e == null) {
            return false;
        } else {
            return true;
        }
    }
}
