/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.joy.io.persist;

import com.sleepycat.bind.EntityBinding;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.Environment;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.joy.io.Connection;
import org.joy.io.DBException;

/**
 * 这是链接数据和操作的管道，是每一个表单面向操作的抽象
 * @author Lamfeeling
 */
public class EntityConnection extends Connection {

    public EntityConnection(Environment env, Class<?> c) throws DBException {
        super(env, c);
    }

    @Override
    public String getDbName() {
        try {
            return store.getPrimaryIndex().getDatabase().getDatabaseName();
        } catch (DatabaseException ex) {
            Logger.getLogger(EntityConnection.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    @Override
    public EntryBinding getKeyBinding() {
        return store.getPrimaryIndex().getKeyBinding();
    }

    /**
     * @deprecated 实体类存取不用Value
     * @return
     */
    @Override
    public EntryBinding getValueBinding() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public EntityBinding getEntityBinding() {
        return store.getPrimaryIndex().getEntityBinding();
    }

    Store getStore() {
        return store;
    }
}
