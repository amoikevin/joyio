package org.joy.io.persist;

import java.lang.reflect.Field;
import java.util.HashMap;

import org.joy.io.DBException;

import com.sleepycat.db.DatabaseException;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.SecondaryKey;

public class Store {

    @PrimaryKey
    private EntityStore entityStore;
    private Class<?> entityClass;
    private HashMap<String, SecondaryIndex<?, ?, ?>> indexMap = new HashMap<String, SecondaryIndex<?, ?, ?>>();
    private PrimaryIndex<?, ?> primaryIndex;
    private String primaryField;

    public Store(EntityStore store, Class<?> entity) throws DBException {
        this.entityStore = store;
        if (entity.getAnnotation(Entity.class) == null) {
            throw new IllegalArgumentException("");
        }
        this.entityClass = entity;
        for (Field f : entity.getDeclaredFields()) {
            if (f.isAnnotationPresent(PrimaryKey.class)) {
                try {
                    primaryIndex = store.getPrimaryIndex(f.getType(), entity);
                    primaryField = f.getName();
                } catch (DatabaseException e) {
                    e.printStackTrace();
                    throw new DBException();
                }
            }
        }
        // 导入索引
        for (Field f : entity.getDeclaredFields()) {
            try {
                if (f.isAnnotationPresent(SecondaryKey.class)) {
                    indexMap.put(f.getName(), store.getSecondaryIndex(
                            primaryIndex, f.getType(), f.getName()));
                }
            } catch (DatabaseException e) {
                e.printStackTrace();
                throw new DBException();
            }
        }
    }

    public EntityStore getEntityStore() {
        return entityStore;
    }

    public String getPrimaryField() {
        return primaryField;
    }

    public <P, E> PrimaryIndex<P, E> getPrimaryIndex() {
        return (PrimaryIndex<P, E>) primaryIndex;
    }

    public HashMap<String, SecondaryIndex<?, ?, ?>> getIndexMap() {
        return indexMap;
    }

    public <PK, SK, E> SecondaryIndex<SK, PK, E> getIndex(String name) {
        return (SecondaryIndex<SK, PK, E>) indexMap.get(name);
    }

    public Class getEntityClass() {
        return entityClass;
    }
}
