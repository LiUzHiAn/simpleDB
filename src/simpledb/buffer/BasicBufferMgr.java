package simpledb.buffer;

import simpledb.file.Block;

/**
 * 管理缓冲池的对象
 *
 * @program: simpleDB
 * @description:
 * @author: LiuZhian
 * @create: 2019-07-05 16:40
 **/
public class BasicBufferMgr {
    private Buffer[] bufferPool;
    private int numAvailable;

    BasicBufferMgr(int buffsNum) {
        bufferPool = new Buffer[buffsNum];
        numAvailable = buffsNum;
        for (int i = 0; i < buffsNum; i++) {
            bufferPool[i] = new Buffer();
        }
    }

    /**
     * Flushes the dirty buffers modified by the specified transaction.
     *
     * @param txNum
     */
    synchronized void flushAll(int txNum) {
        for (Buffer b : bufferPool) {
            if (b.isModifiedBy(txNum)) {
                b.flush();
            }
        }
    }

    /**
     * 将指定块的内容固定到缓冲池上
     *
     * @param blk
     * @return 被固定在具体的哪个缓冲单元上，pin失败返回空
     */
    synchronized Buffer pin(Block blk) {
        // 先去找一遍，看看是否哪个缓冲单元上保存的就是指定blk的内容
        // （无论它是pinned还是unpinned的状态）
        Buffer buffer = findExistingBuffer();
        // 如果不存在，则找一个unpinned的缓冲单元
        if (buffer == null)
        {
            buffer = chooseUnpinnedBuffer();
            if (buffer == null)  // 没有unpinned的缓冲单元
            {
                return null;
            }
            buffer.assignToBlock(blk);  // 找到了就将块中的内容赋到缓冲单元的页上去
        }
        // 如果存在一个缓冲单元上保存的就是这个blk的内容，就不要再去找unpinned的单元了
        if(!buffer.isPinned())  // 如果该页是没被固定的状态，则即将固定，把numAvailable减1
            numAvailable--;
        buffer.pin();  // 该页pin的次数加1（支持多用户并发固定某个块）
        return buffer;

    }

    /**
     * 在指定文件中开辟一个新的块，并pin到一个缓冲单元中
     * @param fileName
     * @param fmtr
     * @return 返回null如果没有缓冲单元
     */
    synchronized Buffer pinNew(String fileName,PageFormatter fmtr)
    {
        Buffer buffer=chooseUnpinnedBuffer();
        if (buffer==null)
            return null;
        buffer.assignToNew(fileName,fmtr);
        numAvailable--;
        buffer.pin();
        return buffer;
    }
    /**
     * unpin指定缓冲单元
     * @param buffer
     */
    synchronized void unpin(Buffer buffer)
    {
        buffer.unpin();
        if(!buffer.isPinned())
            numAvailable++;
    }

    /**
     * 在所有缓冲池中，找一个unpinned的缓冲单元,没有则返回null
     *
     * @return
     */
    private Buffer chooseUnpinnedBuffer() {
        for (Buffer b : bufferPool) {
            if (!b.isPinned())
                return b;
        }
        return null;
    }


    /**
     * 先去找一遍，看看是否哪个缓冲单元上保存的就是指定blk的内容
     * 无论它是pinned还是unpinned的状态.
     *
     * @return
     */
    private Buffer findExistingBuffer() {
        for (Buffer b : bufferPool) {
            Block tempBlock = b.block();
            if (tempBlock != null && tempBlock.equals(b))
                return b;
        }
        return null;
    }


    public int getNumAvailable() {
        return numAvailable;
    }
}
