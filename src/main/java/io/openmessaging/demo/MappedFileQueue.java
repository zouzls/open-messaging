package io.openmessaging.demo;

import io.openmessaging.Message;
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
public class MappedFileQueue {
    private final String storePath;
    private final int fileSize;
    private long commonWritePositon =0L;
    private final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<MappedFile>();
    private Lock lock = new ReentrantReadWriteLock().writeLock();
    private MappedFile writerHandle=null;

    public MappedFileQueue(final String storePath, int fileSize) {
        this.storePath = storePath;
        this.fileSize = fileSize;
    }
    public MappedFileQueue init(){
        File path=new File(System.getProperty("user.dir")+storePath);
        File[] files=path.listFiles();
        MappedFile mappedFile=null;
        for (File f:files){
            if (!f.isDirectory()){
                String nextFilePath = System.getProperty("user.dir")+this.storePath + File.separator +f.getName();
                try {
                    mappedFile = new MappedFile(nextFilePath, this.fileSize);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                mappedFiles.add(mappedFile);
            }

        }
        if (mappedFiles.size()>0)
            return this;
        else
            return null;
    }
    public MappedFile getLastMappedFile() {
        MappedFile mappedFileLast = null;

        while (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
                break;
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                e.printStackTrace();
                break;
            }
        }
        return mappedFileLast;
    }
    public MappedFile getLastMappedFile(final long startOffset) {

        return getLastMappedFile(startOffset, true);
    }
    public MappedFile getLastMappedFile(final long startOffset, boolean needCreate) {
        long createOffset = -1;
        MappedFile mappedFileLast = getLastMappedFile();

        if (mappedFileLast == null) {
            createOffset = startOffset - (startOffset % this.fileSize);
        }

        if (mappedFileLast != null && mappedFileLast.isFull()) {
            createOffset = mappedFileLast.getFileFromOffset() + this.fileSize;
        }

        if (createOffset != -1 && needCreate) {
            String nextFilePath = System.getProperty("user.dir")+this.storePath + File.separator + AllUtils.offset2FileName(createOffset);
            MappedFile mappedFile = null;
            try {
                mappedFile = new MappedFile(nextFilePath, this.fileSize);
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (mappedFile != null) {
                this.mappedFiles.add(mappedFile);
            }
            return mappedFile;
        }
        return mappedFileLast;
    }
    public MappedFile getMappedFileByOffset(final long offset) {
        try {
            MappedFile MappedFile = this.getFirstMappedFile();
            if (MappedFile != null) {
                int index = (int) ((offset / this.fileSize) - (MappedFile.getFileFromOffset() / this.fileSize));

                return this.mappedFiles.get(index);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    public MappedFile getFirstMappedFile() {
        MappedFile MappedFileFirst = null;
        if (!this.mappedFiles.isEmpty()) {
            try {
                MappedFileFirst = this.mappedFiles.get(0);
            } catch (IndexOutOfBoundsException e) {
                //ignore
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return MappedFileFirst;
    }


    public boolean add(byte[] message) throws IOException {
        writerHandle=getLastMappedFile();
        if (writerHandle==null)
            writerHandle=getLastMappedFile(commonWritePositon);
        long newWritePositon = writerHandle.write(message);
        if (writerHandle.isFull()) {
            writerHandle=getLastMappedFile(newWritePositon);
            newWritePositon = writerHandle.write(message);
        }
        this.commonWritePositon =newWritePositon;
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

    public long getCommonWritePositon() {
        return commonWritePositon;
    }

    public Message getMessageByOffset(long commonOffset) {
        MappedFile MappedFile =getMappedFileByOffset(commonOffset);
        long offset=commonOffset%fileSize;
        Message message= MappedFile.getMessageByOffset((int) offset);
        return message;
    }
}


