package org.joy.io.test;

import org.joy.io.DBException;
import org.joy.io.Platform;
import org.joy.io.persist.EntityCommand;
import org.joy.io.persist.EntityConnection;
import org.joy.io.persist.EntityReader;
import org.joy.io.persist.EntitySet;

public class Test2 {

    /**
     * @param args
     * @throws DBException
     * @throws ClassNotFoundException
     */
    public static void main(String[] args) throws DBException, ClassNotFoundException {
        // TODO Auto-generated method stub
        Platform p = new Platform("mp3");

        EntityConnection s = p.open(Singer.class);
        EntityCommand<String, Singer> c1 = new EntityCommand<String, Singer>(s);
        c1.setEntity(new Singer("周杰伦"));
        c1.setEntity(new Singer("张柏芝"));

        EntityConnection ec = p.open(MP3.class);
        EntityCommand<String, MP3> c2 = new EntityCommand<String, MP3>(ec);
        c2.setEntity(new MP3("菊花台", "周杰伦", 123L, 1));
        c2.setEntity(new MP3("听妈妈的话", "周杰伦", 234L, 2));
        c2.setEntity(new MP3("不听妈妈的话", "张柏芝", 345L, 5));

        long start = System.currentTimeMillis();
        EntitySet<MP3> set = c2.excuteSQL(
                "select from org.joy.io.test.MP3 where test>10 and test<20 and singer='周杰伦'");
        System.out.println(System.currentTimeMillis()-start);

        EntityReader<MP3> r = set.getReader();
        while (r.next()) {
            MP3 t = r.getEntity();
            System.out.println(t.getName());
        }
        r.close();
    }
}
