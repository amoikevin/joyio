/*
 * Platform.java
 *
 * Created on 2007年10月19日, 下午7:55
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */
package org.joy.io;

import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.Environment;
import com.sleepycat.db.EnvironmentConfig;
import com.sleepycat.db.LockDetectMode;
import com.sleepycat.db.StatsConfig;

import java.io.File;
import java.util.Vector;
import org.joy.io.config.DefaultDBConfig;
import org.joy.io.persist.EntityConnection;

/**
 * 抽象的数据库平台类，如果要重用该方法，可继承该类，并且重写方法
 * 
 * @author 柳松
 */
public class Platform {

    /**
     * 底层的数据库环境，一个Environment对象
     */
    protected Environment env;
    /**
     * 数据库最大可以提供的抽象层数
     */
    public static int Max_Table = 50;
    /**
     * 数据库的连接池，集中了所有的数据库抽象层
     */
    protected Vector<Connection> connPool = new Vector<Connection>();
    /**
     * 数据库的序列化存储目录，程序员可以选择用，或者不用
     */
    protected StoredClassCatalog sCatalog;

    /**
     * 初始化一个新的数据库平台
     * 
     * @param path
     *            数据库平台路径
     * @param cacheSize
     *            提供给数据库的内存缓冲大小
     * @throws DBException
     *             如果出现数据库错误则返回该错误
     */
    public void init(String path, long cacheSize) throws DBException {
        // setup the environment,see BDB doc for details
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setPrivate(true);

        envConfig.setAllowCreate(true);
        envConfig.setPrivate(true);
        envConfig.setInitializeCache(true);
        envConfig.setInitializeLocking(true);
        envConfig.setLockDetectMode(LockDetectMode.MINWRITE);
        envConfig.setCacheSize(cacheSize);
        // verify the file folder
        try {
            File envFolder = new File("./");
            if (!envFolder.exists()) {
                envFolder.mkdir();
            }
            File workFolder = new File("./" + path);

            if (!workFolder.exists()) {
                workFolder.mkdir();
            }
            // create/open the new environment for our Database
            env = new Environment(workFolder, envConfig);
            // print the information
            System.out.println("Environment Initalize done with cache:" + env.getCacheStats(new StatsConfig()).getBytes());
            // 打开序列化数据库
            Database sDB = env.openDatabase(null, "Catalog", "Catalog",
                    new DefaultDBConfig());
            sCatalog = new StoredClassCatalog(sDB);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new DBException("Error in open/creating ENV！");
        }
    }

    /**
     * 初始化一个新的数据库平台,默认初始Cache大小 32MB
     * 
     * @param path
     *            数据库平台路径
     * @throws DBException
     *             如果出现数据库错误则返回该错误
     */
    public Platform(String path) throws DBException {
        init(path, 32 * 1024 * 1024);
    }

    /**
     * 初始化一个新的数据库平台
     * 
     * @param path
     *            数据库平台路径
     * @param cacheSize
     *            提供给数据库的内存缓冲大小
     * @throws DBException
     *             如果出现数据库错误则返回该错误
     */
    public Platform(String path, long cacheSize) throws DBException {
        init(path, cacheSize);
    }

    /**
     * 返回服务器数据库平台的Environment对象实例
     * 
     * @return 返回服务器数据库平台的Environment对象实例
     */
    public Environment getEnv() {
        return env;
    }

    /**
     * 返回数据库平台的路径名称
     * 
     * @return 返回数据库平台的路径名称
     * @throws daphne.db.DBException
     *             如果数据库错误则抛出该异常
     */
    public String getPath() throws DBException {
        try {
            return env.getHome().getPath();
        } catch (DatabaseException ex) {
            ex.printStackTrace();
            throw new DBException();
        }
    }

    /**
     * 设定数据库平台使用的内存占整个jvm的百分之几
     * 
     * @param percent
     *            百分数
     * @throws daphne.db.DBException
     *             如果发生数据库错误，则抛出该异常
     */
    public void setBufferSize(int percent) throws DBException {
        try {
            EnvironmentConfig config = env.getConfig();
            config.setCacheSize(Runtime.getRuntime().maxMemory() * percent / 100);
            env.setConfig(config);
            System.out.println(env.getCacheStats(new StatsConfig()).getBytes());
        } catch (DatabaseException ex) {
            ex.printStackTrace();
            throw new DBException();
        }
    }

    /**
     * 抽象层工厂。集中了所有抽象层。
     * 
     * @param DBName
     *            要打开的数据库名称
     * @throws org.joy.io.DBException
     *             如果发生数据库错误，则抛出该异常
     * @return 返回一个数据库抽象层
     */
    protected  Connection connectionFactory(String DBName)
            throws DBException{
    	return null;
    }

    /**
     * 抽象层工厂。集中了所有抽象层。
     * 
     * @param entityClass 
     *            要打开的实体类名称
     * @throws org.joy.io.DBException
     *             如果发生数据库错误，则抛出该异常
     * @return 返回一个数据库抽象层
     */
    protected Connection connectionFactory(Class<?> entityClass)
            throws DBException {
        return new EntityConnection(env, entityClass);
    }

    /**
     * 关闭用户数据库。必须被实现的类重写，在这里关闭所有的子类打开的数据库
     * 
     * @throws org.joy.io.DBException
     *             如果发生数据库错误，则抛出该异常
     */
    protected void closeUserDB() throws DBException{};

    /**
     * 同步用户数据库，必须为继承的子类重写，同步所有子类中打开的用户数据库
     * 
     * @throws org.joy.io.DBException
     *             如果发生数据库错误，则抛出该异常
     */
    protected void syncUserDB() throws DBException{}

    /**
     * 同步所有数据库
     * 
     * @throws org.joy.io.DBException
     *             如果发生数据库错误，则抛出该异常
     */
    public void sync() throws DBException {
        syncUserDB();
    }

    /**
     * 同步所有数据库，然后关闭它们
     * 
     * @throws org.joy.io.DBException
     *             如果发生数据库错误，则抛出该异常
     */
    public void syncAndClose() throws DBException {
        sync();
        for (Connection conn : connPool) {
            conn.close();
        }
        closeUserDB();
        try {
            sCatalog.close();
            env.close();
        } catch (DatabaseException ex) {
            ex.printStackTrace();
            throw new DBException();
        }
    }

    public EntityConnection open(Class entityClass) throws DBException, ClassNotFoundException {
        return (EntityConnection) open(entityClass.getName());
    }

    /**
     * 打开一个基于某个数据库的数据库连接抽象层。如果现在的连接数已经大于最大可以提供的连接，则返回Null
     * 
     * @return 数据库抽象连接层
     * @param DBName
     *            要打开的数据库名
     * @throws DBException
     *             如果发生数据库错误，则抛出该异常
     */
    public synchronized Connection open(String DBName) throws DBException, ClassNotFoundException {
        // 找找缓冲当中有没有没有用的这个数据库连接
        for (Connection conn : connPool) {
            if (conn.isFreed() && conn.getDbName().equals(DBName)) {
                conn.setBusy();
                return conn;
            }
        }
        // 如果找不到就新建一个
        if (connPool.size() == Max_Table) {
            // 找找看有没有没有用的,删除一个
            Connection free = null;
            for (Connection conn : connPool) {
                if (conn.isFreed()) {
                    // 关掉这个连接
                    conn.close();
                    free = conn;
                    break;
                }
            }
            // 把这个连接赶出缓冲区
            if (free != null) {
                connPool.remove(free);
            } // 如果没有空闲的
            else {
                return null;
            }
        }
        Connection conn = null;
        if (DBName.indexOf(".") != -1) {
            conn = connectionFactory(Class.forName(DBName));
        } else {
            conn = connectionFactory(DBName);
        }
        connPool.add(conn);
        return conn;
    }

    public Txn beginTransaction(Txn parent) throws DBException {
        try {
            if (parent != null) {
                return new Txn(env.beginTransaction(parent.getTxn(), null));
            } else {
                return new Txn(env.beginTransaction(null, null));
            }
        } catch (DatabaseException ex) {
            ex.printStackTrace();
            throw new DBException();
        }
    }
}
