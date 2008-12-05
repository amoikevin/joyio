/*
 * DB.java
 *
 * Created on 2007年5月6日, 下午7:14
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */
package org.joy.io;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.Environment;
import com.sleepycat.db.SecondaryDatabase;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.StoreConfig;
import com.sleepycat.persist.evolve.IncompatibleClassException;
import java.util.Hashtable;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.joy.io.persist.Store;

/**
 * 表示数据库数据和操作的中间层。作为数据的抽象
 * @author AC
 */
public abstract class Connection {

    private Vector<Reader> readerPool = new Vector<Reader>();
    private Environment env;
    /**
     * 这个抽象层对应的底层数据库Database对象
     */
    protected Database db;
    private boolean freed;
    private Hashtable<String, SecondaryDatabase> secTable = new Hashtable<String, SecondaryDatabase>();
    //用来存储Entity的Store,其他的Connection为null
    protected Store store;

    protected void addSecDb(String name, SecondaryDatabase db) {
        secTable.put(name, db);
    }

    protected SecondaryDatabase getSecDb(String name) {
        return secTable.get(name);
    }

    public SecondaryConnection getSecondaryConn(final String name) throws DBException {
        SecondaryDatabase sDb = getSecDb(name);
        final Connection outter = this;
        SecondaryConnection conn = new SecondaryConnection(env, sDb) {

            @Override
            public String getDbName() {
                return name;
            }

            @Override
            public EntryBinding getKeyBinding() {
                try {
                    SecDbKeyCreator keyCreator = (SecDbKeyCreator) ((SecondaryDatabase) db).getSecondaryConfig().getKeyCreator();
                    return keyCreator.getSecKeyBinding();
                } catch (DatabaseException ex) {
                    Logger.getLogger(Connection.class.getName()).log(Level.SEVERE, null, ex);
                }
                return null;
            }

            @Override
            public EntryBinding getValueBinding() {
                return outter.getValueBinding();
            }

            @Override
            public EntryBinding getPrimaryKeyBinding() {
                return outter.getKeyBinding();
            }
        };
        return conn;
    }

    /**
     * 创建一个数据库的抽象接口，如果该数据库不存在，则创建一个新的数据库
     * @param env 拥有该数据库的数据库环境
     * @param DBname 该数据库名字
     * @param dbConfig 打开该数据库所用的配置
     * @throws org.joy.io.DBException 如果发生数据库错误，则抛出该异常
     */
    protected Connection(Environment env, String DBname, DatabaseConfig dbConfig) throws DBException {
        this.env = env;
        try {
            if (env != null) //打开主数据库
            {
                db = env.openDatabase(null, DBname, DBname, dbConfig);
            } else {
                db = new Database(DBname, DBname, dbConfig);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new DBException("Can't open");
        }
    }

    protected Connection(Environment env, SecondaryDatabase db) {
        this.env = env;
        this.db = db;
    }

    protected Connection(Environment env, Class<?> c) throws DBException {
        try {
            this.env = env;
            String storeName = c.getName().substring(0, c.getName().lastIndexOf("."));
            StoreConfig conf = new StoreConfig();
            conf.setAllowCreate(true);
            conf.setReadOnly(false);
            store = new Store(new EntityStore(env, storeName, conf), c);
            db = store.getPrimaryIndex().getDatabase();
            //添加二级数据库
            for (String sdbName : store.getIndexMap().keySet()) {
                addSecDb(sdbName, store.getIndexMap().get(sdbName).getDatabase());
            }

        } catch (DatabaseException ex) {
            Logger.getLogger(Connection.class.getName()).log(Level.SEVERE, null, ex);
            throw new DBException();
        } catch (IncompatibleClassException ex) {
            Logger.getLogger(Connection.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * 关闭抽象层
     * @throws daphne.db.DBException 如果发生数据库错误，则抛出该异常
     */
    public void close() throws DBException {
        try {
            if (store != null) {
                store.getEntityStore().close();
            } else {
                for (SecondaryDatabase secDb : secTable.values()) {
                    secDb.close();
                }
                db.close();
            }
        } catch (DatabaseException ex) {
            ex.printStackTrace();
            throw new DBException("关闭错误");
        }
    }

    /**
     * 同步数据
     * @throws daphne.db.DBException 如果发生数据库错误，则抛出该异常
     */
    public void sync() throws DBException {
        try {
            db.sync();
            for (SecondaryDatabase secDb : secTable.values()) {
                secDb.sync();
            }
        } catch (DatabaseException ex) {
            ex.printStackTrace();
            throw new DBException("同步错误");
        }
    }

    /**
     * 获取Database句柄
     * @return 返回数据库对象
     */
    public Database getDb() {
        return db;
    }

    /**
     * 是否被使用，在操作
     * @return 返回 该连接是否被使用
     */
    public boolean isFreed() {
        return freed;
    }

    /**
     * 设置该抽象层是否被使用
     */
    public void setFree() throws DBException {
        this.freed = true;
        for (Reader reader : readerPool) {
            reader.close();
        }
    }

    void setBusy() {
        this.freed = false;
    }

    /**
     * 获取该抽象层所代表的数据库的友好名字
     * @return 返回名字
     */
    public abstract String getDbName();

    /**
     * 获取该拥有抽象层所代表的数据库的数据库环境名
     * @return 返回数据库环境
     */
    public Environment getEnv() {
        return env;
    }

    /**
     * @deprecated 被废除
     * 预读数据库到内存中（尽可能多）这个方法被废除，没有替代
     * @throws daphne.db.DBException 如果发生数据库错误，则抛出该异常
     */
    public void preload() throws DBException {
//        PreloadConfig config = new PreloadConfig();
//        config.setLoadLNs(true);
//        try {
//            db.preload(config);
//        } catch (DatabaseException ex) {
//            ex.printStackTrace();
//            throw new DBException();
//        }
        throw new UnsupportedOperationException();
    }

    /**
     * 获取该抽象层所代表的数据库的键绑定
     * @return 返回该连接所代表的数据库键绑定
     */
    public abstract EntryBinding getKeyBinding();

    /**
     * 获取该抽象层所代表的数据库的值绑定
     * @return 返回值绑定
     */
    public abstract EntryBinding getValueBinding();

    protected void finalize() throws Throwable {
        super.finalize();
        setFree();
    }

    public Vector<Reader> getReaderPool() {
        return readerPool;
    }

    public Hashtable<String, SecondaryDatabase> getSecTable() {
        return secTable;
    }
}
