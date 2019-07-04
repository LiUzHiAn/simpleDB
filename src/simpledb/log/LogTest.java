package simpledb.log;

import simpledb.server.SimpleDB;

import java.util.Iterator;

/**
 * @program: simpleDB
 * @description:
 * @author: LiuZhian
 * @create: 2019-07-04 21:35
 **/
public class LogTest {
    public static void main(String[] args) {
        SimpleDB.init("studentDB");
        LogMgr logMgr = SimpleDB.getLogMgr();

        int lsn1 = logMgr.append(new Object[]{"a", "b"});
        int lsn2 = logMgr.append(new Object[]{"aa", "bb"});


        logMgr.flush(lsn2);

        Iterator<BasicLogRecord> it = logMgr.iterator();
        while (it.hasNext()) {
            BasicLogRecord rec = it.next();
            String v1 = rec.nextString();
            String v2 = rec.nextString();
            System.out.println("[" + v1 + " " + v2 + "]");

        }
    }
}
