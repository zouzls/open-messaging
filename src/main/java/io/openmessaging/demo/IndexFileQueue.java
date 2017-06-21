package io.openmessaging.demo;

import io.openmessaging.util.AllUtils;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author zouzl
 * @create 2017-05-09-23:56
 */
public class IndexFileQueue {
    private final String storePath;
    private final int fileSize;
    private long writePositon =0L;
    private final CopyOnWriteArrayList<IndexFile> indexFiles = new CopyOnWriteArrayList<IndexFile>();
    private Lock lock = new ReentrantReadWriteLock().writeLock();
    private IndexFile writerHandle=null;

    public IndexFileQueue(final String storePath, int fileSize) {
        this.storePath = storePath;
        this.fileSize = fileSize;
    }

    public IndexFileQueue init() {
        File path=new File(System.getProperty("user.dir")+storePath);
        File[] files=path.listFiles();
        if (files==null)
            return null;
        IndexFile indexFile=null;
        for (File f:files){
            String nextFilePath = System.getProperty("user.dir")+this.storePath + File.separator +f.getName();
            try {
                indexFile=new IndexFile(nextFilePath,fileSize);
            } catch (IOException e) {
                e.printStackTrace();
            }
            indexFiles.add(indexFile);
        }
        if (path.exists()&&files.length>0)
            return this;
        else
            return null;
    }

    public IndexFile getLastIndexFile() {
        IndexFile indexFileLast = null;

        while (!this.indexFiles.isEmpty()) {
            try {
                indexFileLast = this.indexFiles.get(this.indexFiles.size() - 1);
                break;
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                e.printStackTrace();
                break;
            }
        }
        return indexFileLast;
    }
    public IndexFile getLastIndexFile(final long startOffset) {
        return getLastIndexFile(startOffset, true);
    }
    public IndexFile getLastIndexFile(final long startOffset, boolean needCreate) {
        long createOffset = -1;
        IndexFile indexFileLast = getLastIndexFile();

        if (indexFileLast == null) {
            createOffset = startOffset - (startOffset % this.fileSize);
        }

        if (indexFileLast != null && indexFileLast.isFull()) {
            createOffset = indexFileLast.getFileFromOffset() + this.fileSize;
        }

        if (createOffset != -1 && needCreate) {
            String nextFilePath = System.getProperty("user.dir")+this.storePath + File.separator + AllUtils.offset2FileName(createOffset);
            IndexFile indexFile = null;
            try {
                indexFile = new IndexFile(nextFilePath, this.fileSize);
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (indexFile != null) {
                this.indexFiles.add(indexFile);
            }
            return indexFile;
        }
        return indexFileLast;
    }
    public IndexFile getIndexFileByOffset(final long offset) {
        try {
            IndexFile indexFile = this.getFirstIndexFile();
            if (indexFile != null) {
                int index = (int) ((offset / this.fileSize) - (indexFile.getFileFromOffset() / this.fileSize));

                return this.indexFiles.get(index);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public IndexFile getFirstIndexFile() {
        IndexFile indexFileFirst = null;
        if (!this.indexFiles.isEmpty()) {
            try {
                indexFileFirst = this.indexFiles.get(0);
            } catch (IndexOutOfBoundsException e) {
                //ignore
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return indexFileFirst;
    }


    public boolean add(byte[] bytes) throws IOException {
        writerHandle= getLastIndexFile();
        if (writerHandle==null)
            writerHandle= getLastIndexFile(writePositon);
        long offset = writerHandle.write(bytes);
        if (writerHandle.isFull()) {
            writerHandle= getLastIndexFile(offset);
            offset = writerHandle.write(bytes);
        }
        this.writePositon =offset;
        return true;
    }


    public boolean offer(byte[] bytes) {
        try {
            lock.lock();
            add(bytes);
            return true;
        } catch (IOException e1) {
            e1.printStackTrace();
        } finally {
            lock.unlock();
        }
        return false;
    }

    public long getWritePositon() {
        return writePositon;
    }

    public long findMsgOffsetByIndex(int indexOffset) {
        IndexFile indexFile = getIndexFileByOffset(indexOffset);
        int offset= indexOffset % fileSize;
        long commonMsgOffset= indexFile.getMessageOffset(offset);
        return commonMsgOffset;
    }
}


