package org.joy.io;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.db.Cursor;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.LockMode;
import com.sleepycat.db.OperationStatus;
import java.util.Vector;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 * 始终只想指定Key的value,如果下一个Item的key不是指定的Key，那么next返回false;
 * 也可以在初始化同时设定Key,和Value，实现键和值的查找。
 * 这个类始始终由Command的Search方法返回。
 * @author 海
 */
public class SearchResultReader extends SequentialReader {

    boolean firstRead = true;
    boolean searchBoth = false;

    /**
     * 用键初始化指针
     * @param cursor
     * @param readerPool
     * @param keyE
     * @param keyBinding
     * @param valueBinding
     */
    public SearchResultReader(Cursor cursor,
            Vector<Reader> readerPool,
            DatabaseEntry keyE,
            EntryBinding keyBinding,
            EntryBinding valueBinding) {
        super(cursor, readerPool, keyBinding, valueBinding);
        this.currentKey = keyE;
    }

    /**
     * 同时用键或值初始化指针
     * @param cursor
     * @param readerPool
     * @param keyE
     * @param valueE
     * @param keyBinding
     * @param valueBinding
     */
    public SearchResultReader(Cursor cursor,
            Vector<Reader> readerPool,
            DatabaseEntry keyE,
            DatabaseEntry valueE,
            EntryBinding keyBinding,
            EntryBinding valueBinding) {
        super(cursor, readerPool, keyBinding, valueBinding);
        this.currentKey = keyE;
        this.currentVal = valueE;
        this.searchBoth = true;
    }

    @Override
    public boolean next(LockMode lockMode) throws DBException {
        try {
            //如果给定了值，则进行双重匹配查询
            if (firstRead) {
                if (searchBoth) {
                    if (cursor.getSearchBoth(currentKey, currentVal, lockMode) != OperationStatus.SUCCESS) {
                        return false;
                    }
                } else {
                    //如果是第一次读取，首先要进行搜索
                    if (cursor.getSearchKey(currentKey, currentVal, lockMode) != OperationStatus.SUCCESS) {
                        return false;
                    }
                }
            } else {
                if (cursor.getNextDup(currentKey, currentVal, lockMode) != OperationStatus.SUCCESS) {
                    return false;
                }
            }
            firstRead = false;
            return true;

        } catch (DatabaseException ex) {
            throw new DBException();
        }
    }
}
