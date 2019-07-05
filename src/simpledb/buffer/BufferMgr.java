package simpledb.buffer;

import simpledb.file.Block;

/**
 * 缓冲池管理对象，对BasicBufferMgr进行了包装。
 * 提供的方法都类似，就是pin()和PinNew()不会返回null。
 * <p>
 * 此外，该类还维护了一个队列，当缓冲池满了，请求的线程会被放到队列中，等待时间为MAX_TIME
 *
 * @program: simpleDB
 * @description:
 * @author: LiuZhian
 * @create: 2019-07-05 16:07
 **/
public class BufferMgr {

    private static final long MAX_TIME = 10000; // 最长等待时间
    private BasicBufferMgr baiscBufferMgr;

    public BufferMgr(int buffSize) {
        baiscBufferMgr = new BasicBufferMgr(buffSize);
    }

    public synchronized Buffer pin(Block blk) {
        try {
            long timestamp = System.currentTimeMillis();
            Buffer buff = baiscBufferMgr.pin(blk);
            while (buff == null && !waitingTooLong(timestamp)) {
                wait(MAX_TIME);  //

                buff = baiscBufferMgr.pin(blk);
            }
            // 等待超时
            if (buff == null)
                throw new BufferAbortException();
            return buff;
        } catch (InterruptedException e) {
            throw new BufferAbortException();
        }
    }

    /**
     * unpin具体的缓冲单元，如果unpin后该缓冲单元的pin count=0，
     * 则通知唤醒等待队列上的进程
     *
     * @param buffer
     */
    public synchronized void unpin(Buffer buffer) {
        baiscBufferMgr.unpin(buffer);
        if (!buffer.isPinned()) {
            notifyAll();
        }

    }

    /**
     * 将指定事务相关的所有脏缓冲页写回磁盘
     */
    public void flushAll(int txNum) {
        baiscBufferMgr.flushAll(txNum);
    }

    /**
     * 得到缓冲池中可用的缓存单元数量
     *
     * @return
     */
    public int available() {
        return baiscBufferMgr.getNumAvailable();
    }

    private synchronized Buffer pinNew(String fileName, PageFormatter fmtr) {
        try {
            long timestamp = System.currentTimeMillis();
            Buffer buff = baiscBufferMgr.pinNew(fileName, fmtr);
            while (buff == null && !waitingTooLong(timestamp)) {
                wait(MAX_TIME);  //

                buff = baiscBufferMgr.pinNew(fileName, fmtr);
            }
            // 等待超时
            if (buff == null)
                throw new BufferAbortException();
            return buff;
        } catch (InterruptedException e) {
            throw new BufferAbortException();
        }
    }


    private boolean waitingTooLong(long starttime) {
        return (System.currentTimeMillis() - starttime) > MAX_TIME;
    }
}
