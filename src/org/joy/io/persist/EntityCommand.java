/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.joy.io.persist;

import com.sleepycat.bind.EntityBinding;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.db.CursorConfig;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.persist.EntityCursor;

import java.util.Map;
import java.util.SortedMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.joy.io.DBException;

/**
 * EntityCommand 负责所有和Entity有关的数据库操作， 注意，这个类不继承Command类，但是提供类似的操作
 * 
 * @author Lamfeeling
 */
public class EntityCommand<PK, E> {

    private Store store;

    /**
     * 初始化一个实例
     *
     * @param conn
     *            一个指向Entity表格的连接
     */
    public EntityCommand(EntityConnection conn) {
        store = conn.getStore();
    }

    /**
     * 添加一个Entity
     *
     * @param entity
     *            Entity类的对象，这个Entity类必须是有Entity annotation标注的
     * @throws org.joy.io.DBException
     *             如果出现任何数据库错误，抛出这个异常
     */
    public void setEntity(E entity) throws DBException {
        try {
            store.getPrimaryIndex().put(entity);
        } catch (DatabaseException ex) {
            Logger.getLogger(EntityCommand.class.getName()).log(Level.SEVERE,
                    null, ex);
            throw new DBException();
        }
    }

    /**
     * 按照主键删除一个数据库项目
     *
     * @param key
     *            主键
     * @throws DBException
     *             数据库的异常
     */
    public void deleteEntity(PK key) throws DBException {
        try {
            store.getPrimaryIndex().delete(key);
        } catch (DatabaseException ex) {
            Logger.getLogger(EntityCommand.class.getName()).log(Level.SEVERE,
                    null, ex);
            throw new DBException();
        }
    }

    /**
     * 按照主键获取一个数据
     *
     * @param key
     *            主键
     * @return 返回的数据
     * @throws DBException
     *             数据库异常时抛出
     */
    public E getEntity(PK key) throws DBException {
        try {
            return (E) store.getPrimaryIndex().get(key);
        } catch (DatabaseException ex) {
            Logger.getLogger(EntityCommand.class.getName()).log(Level.SEVERE,
                    null, ex);
            throw new DBException();
        }
    }

    public Map<PK, E> map() {
        return (Map<PK, E>) store.getPrimaryIndex().map();
    }

    public SortedMap<PK, E> sortedMap() {
        return (SortedMap<PK, E>) store.getPrimaryIndex().sortedMap();
    }

    /**
     * @deprecated 逻辑错误，主键是唯一的，所以这个方法相当于getEntity方法。请用该方法替代 按照主键返回一个游标
     * @param key
     *            主键值
     * @return 游标
     * @throws DBException
     *             如果异常时抛出
     */
    public EntityReader<E> search(PK key) throws DBException {
        try {
            EntityCursor<E> c = (EntityCursor<E>) store.getPrimaryIndex().entities(null, key, true, key, true, CursorConfig.DEFAULT);
            return new EntityReader<E>(c);
        } catch (DatabaseException ex) {
            Logger.getLogger(EntityCommand.class.getName()).log(Level.SEVERE,
                    null, ex);
            throw new DBException();
        }
    }

    /**
     * 按照索引返回游标
     *
     * @param <SK>
     *            索引类型
     * @param indexField
     *            索引字段名称
     * @param key
     *            索引键值
     * @return 数据游标
     * @throws DBException
     *             如果发生不可逆转异常时抛出
     */
    public <SK> EntityReader<E> search(String indexField, SK key)
            throws DBException {
        try {
            EntityCursor<E> c = (EntityCursor<E>) store.getIndex(indexField).entities(null, key, true, key, true, CursorConfig.DEFAULT);
            return new EntityReader<E>(c);
        } catch (DatabaseException ex) {
            Logger.getLogger(EntityCommand.class.getName()).log(Level.SEVERE,
                    null, ex);
            throw new DBException();
        }
    }

    /**
     * 搜索键值小于当前键值的数据
     * @param <SK> 要搜索键的类型
     * @param indexField 要搜索的字段名称
     * @param key 键值
     * @return 一个指向键值的游标
     * @throws org.joy.io.DBException 如果发生不可逆转的错误，则抛出这个异常
     */
    public <SK> EntityReader<E> searchLessThan(String indexField, SK key)
            throws DBException {
        try {
            EntityCursor<E> c = (EntityCursor<E>) store.getIndex(indexField).entities(null, null, false, key, true,
                    CursorConfig.DEFAULT);
            return new EntityReader<E>(c);
        } catch (DatabaseException ex) {
            Logger.getLogger(EntityCommand.class.getName()).log(Level.SEVERE,
                    null, ex);
            throw new DBException();
        }
    }

    public EntitySet<E> excuteSQL(String SQL) throws DBException {
        try {
            SQLStatement statement = new SQLStatement(SQL);

            EntityBinding eBinding = store.getPrimaryIndex().getEntityBinding();
            EntryBinding keyBinding = store.getPrimaryIndex().getKeyBinding();

            EntitySetConfig config = new EntitySetConfig(eBinding,
                    keyBinding);

            EntitySet<E> set = null;
            for (String indexName : statement.getBounds().keySet()) {
                Object lower = statement.getBounds().get(indexName).lower;
                Object upper = statement.getBounds().get(indexName).upper;
                boolean lowIn = statement.getBounds().get(indexName).lowInclusive;
                boolean upIn = statement.getBounds().get(indexName).upInclusive;
                
                EntityCursor<E> c = null;
                // 如果是键
                if (store.getIndex(indexName) != null) {
                    c = (EntityCursor<E>) store.getIndex(indexName).entities(
                            null, lower, lowIn, upper, upIn,
                            CursorConfig.DEFAULT);
                // 如果是主键
                } else if (store.getPrimaryField().equals(indexName)) {
                    c = (EntityCursor<E>) store.getPrimaryIndex().entities(
                            null, lower, lowIn, upper, upIn,
                            CursorConfig.DEFAULT);
                } else {
                    throw new DBException("没有这个索引键！");
                }
                // 做交集运算
                if (set == null) {
                    set = new EntitySet<E>(c, config);
                } else {
                    EntitySet<E> t = new EntitySet<E>(c, config);
                    set.join(t);
                }
            }
            return set;
        } catch (Exception e) {
            e.printStackTrace();
            throw new DBException();
        }
    }
}
