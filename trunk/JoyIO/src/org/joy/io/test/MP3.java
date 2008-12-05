/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.joy.io.test;


import com.sleepycat.persist.model.DeleteAction;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

/**
 *
 * @author Lamfeeling
 */
@Entity
public class MP3 {

    @PrimaryKey
    private String name;
    @SecondaryKey(relate = Relationship.MANY_TO_ONE,
    relatedEntity = Singer.class, onRelatedEntityDelete = DeleteAction.CASCADE)
    private String singer;
    @SecondaryKey(relate = Relationship.MANY_TO_ONE)
    private Long time;
    @SecondaryKey(relate = Relationship.MANY_TO_ONE)
    Integer test;
    public MP3() {
    }

    public MP3(String name, String singer, Long time, int test) {
        this.name = name;
        this.singer = singer;
        this.time = time;
        this.test = test;
    }

    public String getName() {
        return name;
    }

    public String getSinger() {
        return singer;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setSinger(String singer) {
        this.singer = singer;
    }
    public void setTime(Long time) {
		this.time = time;
	}
    public Long getTime() {
		return time;
	}
 }
