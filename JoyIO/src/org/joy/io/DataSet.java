/*
 * DataSet.java
 *
 * Created on 2007年6月1日, 上午11:46
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */
package org.joy.io;

import com.sleepycat.bind.EntityBinding;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.db.Cursor;
import com.sleepycat.db.CursorConfig;
import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.DatabaseType;
import com.sleepycat.db.Environment;
import com.sleepycat.db.EnvironmentConfig;
import com.sleepycat.db.HashStats;
import com.sleepycat.db.LockDetectMode;
import com.sleepycat.db.LockMode;
import com.sleepycat.db.OperationStatus;
import com.sleepycat.db.StatsConfig;
import java.util.Comparator;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.joy.io.config.DataSetConfig;

/**
 * 一个简单的大容量有序表
 * 
 * @author AC
 */
public class DataSet implements Comparable<DataSet> {
	/**
	 * 当前Set的实例个数
	 */
	private static int NUM_ALIVE;
	/**
	 * 默认的数据集内存占用大小:1M
	 */
	public static int DEFAULT_BUFFER_SIZE = 1024 * 1024;
	/**
	 * 数据库的Env
	 */
	private Environment env;
	protected Database db;
	private EntryBinding keyBinding;
	private EntryBinding valueBinding;
	private EntityBinding eBinding;
	private Vector<Reader> readerPool = new Vector<Reader>();

	/**
	 * 创建数据库环境
	 * 
	 * @param bufferSize
	 *            缓冲区大小
	 * @throws DBException
	 *             如果出现任何数据库问题排除这个异常
	 */
	private void createEnvironment(long bufferSize) throws DBException {
		try {
			if (env == null) {
				EnvironmentConfig envConfig = new EnvironmentConfig();
				envConfig.setPrivate(true);

				envConfig.setAllowCreate(true);
				envConfig.setInitializeCDB(true);
				envConfig.setInitializeCache(true);
				envConfig.setLockDetectMode(LockDetectMode.MINWRITE);
				envConfig.setCacheSize(bufferSize);
				env = new Environment(null, envConfig);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new DBException();
		}
	}

	/**
	 * 创建实例
	 * 
	 * @param keyBinding
	 *            键的绑定
	 * @param valueBinding
	 *            值的绑定
	 * @throws DBException
	 *             如果有异常
	 */
	public DataSet(EntryBinding keyBinding, EntryBinding valueBinding)
			throws DBException {
		createEnvironment(DEFAULT_BUFFER_SIZE);
		// 创建索引数据库
		DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setType(DatabaseType.HASH);
		dbConfig.setAllowCreate(true);
		try {
			db = env.openDatabase(null, null, null, dbConfig);
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new DBException();
		}
		this.keyBinding = keyBinding;
		this.valueBinding = valueBinding;
		NUM_ALIVE++;
	}

	/**
	 * 创建实例， 从一个Reader导入数据
	 * 
	 * @param reader
	 *            用于到数据的Reader
	 * @throws DBException
	 *             如果出现底层异常，抛出这个异常
	 */
	public DataSet(Reader reader) throws DBException {
		createEnvironment(DEFAULT_BUFFER_SIZE);
		// 创建索引数据库
		DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setType(DatabaseType.HASH);
		dbConfig.setAllowCreate(true);
		try {
			db = env.openDatabase(null, null, null, dbConfig);
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new DBException();
		}
		this.keyBinding = reader.getKeyBinding();
		this.valueBinding = reader.getValueBinding();
		NUM_ALIVE++;
		put(reader);
	}

	/**
	 * 创建实例
	 * 
	 * @param keyBinding
	 *            键绑定
	 * @param valueBinding
	 *            值绑定
	 * @param dupComparator
	 *            重复键的比较子，如果为null,则默认用字节比较法
	 * @throws DBException
	 *             如果出现底层异常抛出之
	 */
	public DataSet(EntryBinding keyBinding, EntryBinding valueBinding,
			Comparator dupComparator) throws DBException {
		createEnvironment(DEFAULT_BUFFER_SIZE);
		// 创建索引数据库
		DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setType(DatabaseType.HASH);
		dbConfig.setAllowCreate(true);
		dbConfig.setSortedDuplicates(true);
		if (dupComparator != null) {
			dbConfig.setDuplicateComparator(dupComparator);
		}
		try {
			db = env.openDatabase(null, null, null, dbConfig);
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new DBException();
		}
		this.keyBinding = keyBinding;
		this.valueBinding = valueBinding;
		NUM_ALIVE++;
	}

	/**
	 * 创建实例
	 * 
	 * @param config
	 *            创建实例的详细配置
	 * @throws DBException
	 *             如果出现底层异常抛出之
	 */
	public DataSet(DataSetConfig config) throws DBException {
		createEnvironment(config.getBufferSize());
		// 创建索引数据库
		DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setType(config.getType());
		dbConfig.setAllowCreate(true);

		if (config.isSupportDup()) {
			dbConfig.setSortedDuplicates(true);
			if (config.getComparator() != null) {
				dbConfig.setDuplicateComparator(config.getComparator());
			}
		}
		try {
			db = env.openDatabase(null, null, null, dbConfig);
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new DBException();
		}
		this.keyBinding = config.getKeyBinding();
		this.valueBinding = config.getValBinding();
		NUM_ALIVE++;
	}

	/**
	 * 关闭当前的结果集，释放资源
	 * @throws DBException 如果出现底层异常抛出之
	 */
	public void close() throws DBException {
		try {
			for (int i = 0; i < readerPool.size(); i++) {
				readerPool.get(i).close();
			}
			db.close();
			NUM_ALIVE--;
			if (NUM_ALIVE == 0) {
				env.close();
			}
		} catch (DatabaseException ex) {
			ex.printStackTrace();
			throw new DBException();
		}
	}

	/**
	 * 判断是否有指定的Key存在于数据集当中
	 * @param key 指定的Key
	 * @return
	 * @throws DBException 如果出现底层异常抛出之
	 */
	public boolean contains(Object key) throws DBException {
		try {
			DatabaseEntry keyE = new DatabaseEntry();
			getKeyBinding().objectToEntry(key, keyE);

			return db.get(null, keyE, new DatabaseEntry(), LockMode.DEFAULT) == OperationStatus.SUCCESS;
		} catch (DatabaseException ex) {
			Logger.getLogger(DataSet.class.getName()).log(Level.SEVERE, null,
					ex);
			throw new DBException();
		}
	}

	/**
	 * 获取数据集当中指定键的纪录
	 * 
	 * @param key
	 *            要获取的记录的键
	 * @return 返回要获取的纪录值
	 */
	public Object get(Object key) throws DBException {
		DatabaseEntry keyE = new DatabaseEntry();
		keyBinding.objectToEntry(key, keyE);
		DatabaseEntry valueE = new DatabaseEntry();
		OperationStatus retVal;
		try {
			retVal = db.get(null, keyE, valueE, LockMode.DEFAULT);
		} catch (DatabaseException ex) {
			ex.printStackTrace();
			throw new DBException();
		}
		if (retVal == OperationStatus.NOTFOUND) {
			return null;
		}
		return valueBinding.entryToObject(valueE);
	}

	public Database getDB() {
		return db;
	}

	/**
	 * 测试方法，打印所有的数据集内容到控制台
	 * @throws DBException
	 */
	public void printAll() throws DBException {
		try {
			Cursor c = db.openCursor(null, null);
			DatabaseEntry keyE = new DatabaseEntry();
			DatabaseEntry valueE = new DatabaseEntry();
			while (c.getNext(keyE, valueE, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
				Object key = keyBinding.entryToObject(keyE);
				Object value = keyBinding.entryToObject(valueE);
				System.out.println(key + ":" + value);
			}
		} catch (DatabaseException ex) {
			ex.printStackTrace();
			throw new DBException();
		}
	}

	/**
	 * 向数据集当中添加一个记录
	 * 
	 * @param key
	 *            要添加的纪录的键
	 * @param value
	 *            要添加的纪录的值
	 */
	public void put(Object key, Object value) throws DBException {
		try {
			if(valueBinding == null){
				throw new DBException("Entity不适合使用Key Value方式复制");
			}
			DatabaseEntry keyE = new DatabaseEntry();
			keyBinding.objectToEntry(key, keyE);
			DatabaseEntry valueE = new DatabaseEntry();
			valueBinding.objectToEntry(value, valueE);
			db.put(null, keyE, valueE);
		} catch (DatabaseException ex) {
			ex.printStackTrace();
			throw new DBException();
		}
	}

	/**
	 * 从数据集导入文件
	 * @param set 
	 * @throws DBException 如果出现底层异常抛出之
	 */
	public void put(DataSet set) throws DBException {
		try {
			Cursor setC = set.db.openCursor(null, null);
			DatabaseEntry keyE = new DatabaseEntry();
			DatabaseEntry valueE = new DatabaseEntry();
			while (setC.getNext(keyE, valueE, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
				db.put(null, keyE, valueE);
			}
			setC.close();
		} catch (DatabaseException ex) {
			ex.printStackTrace();
			throw new DBException();
		}
	}

	public void put(Reader reader) throws DBException {
		while (reader.next()) {
			put(reader.getKey(), reader.getValue());
		}
	}

	public Reader getReader(CursorConfig cursorConfig) throws DBException {
		try {
			Cursor c = db.openCursor(null, cursorConfig);
			synchronized (readerPool) {
				Reader r = new SequentialReader(c, readerPool, keyBinding,
						valueBinding);
				readerPool.add(r);
				return r;
			}
		} catch (DatabaseException ex) {
			ex.printStackTrace();
			throw new DBException();
		}
	}

	public Reader getReader() throws DBException {
		return getReader(null);
	}


	public Reader search(Object key, boolean writeCursor) throws DBException {
		try {
			CursorConfig cursorConfig = new CursorConfig();
			if (writeCursor) {
				cursorConfig.setWriteCursor(true);
			}
			Cursor c = db.openCursor(null, cursorConfig);
			DatabaseEntry keyE = new DatabaseEntry();
			keyBinding.objectToEntry(key, keyE);

			Reader r = new SearchResultReader(c, readerPool, keyE, keyBinding,
					valueBinding);
			return r;
		} catch (DatabaseException ex) {
			ex.printStackTrace();
			throw new DBException();
		}
	}

	public Reader search(Object key) throws DBException {
		return search(key, false);
	}

	public void remove(Object key) throws DBException {
		try {
			DatabaseEntry keyE = new DatabaseEntry();
			keyBinding.objectToEntry(key, keyE);
			OperationStatus retVal = db.delete(null, keyE);
			if (retVal == OperationStatus.NOTFOUND) {
				return;
			}
		} catch (DatabaseException ex) {
			ex.printStackTrace();
			throw new DBException();
		}
	}

	/**
	 * 获取数据项个数
	 * 
	 * @deprecated 不可靠方法
	 * @return
	 * @throws DBException
	 */
	public int count() throws DBException {
		try {
			StatsConfig conf = new StatsConfig();
			conf.setFast(true);
			HashStats stats = (HashStats) db.getStats(null, conf);
			return stats.getNumData();
		} catch (DatabaseException ex) {
			Logger.getLogger(DataSet.class.getName()).log(Level.SEVERE, null,
					ex);
			throw new DBException();
		}
	}

	/**
	 * 清除数据库所有项目
	 * 
	 * @throws DBException
	 */
	public void clear() throws DBException {
		try {
			db.truncate(null, false);
		} catch (DatabaseException ex) {
			Logger.getLogger(DataSet.class.getName()).log(Level.SEVERE, null,
					ex);
			throw new DBException();
		}
	}

	/**
	 * 和另外的数据集比个数多少
	 * 
	 * @deprecated 不可靠方法
	 */
	public int compareTo(DataSet set) {
		try {
			return count() - set.count();
		} catch (DBException ex) {
			Logger.getLogger(DataSet.class.getName()).log(Level.SEVERE, null,
					ex);
			return 0;
		}
	}

	public EntryBinding getKeyBinding() {
		return keyBinding;
	}

	public EntryBinding getValueBinding() {
		return valueBinding;
	}
	
	public static void main(String []args) throws DBException{
		DataSet set = new DataSet(new StringBinding(),new StringBinding());
		set.put("Joy", "Search");
		set.printAll();
		set.close();
	}
}
