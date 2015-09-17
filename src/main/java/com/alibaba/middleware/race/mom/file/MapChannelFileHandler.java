/**
 * 
 */
package com.alibaba.middleware.race.mom.file;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import sun.nio.ch.DirectBuffer;

/**
 * @author showki
 *
 */
public class MapChannelFileHandler implements FileHandler{

	private MappedByteBuffer mbb = null;
	private FileChannel channel = null;  
    private RandomAccessFile fs = null; 
    private boolean isForce = false;
    private int fileMaxLength = 40*1024*1024;	// 40MB
	
	/* (non-Javadoc)
	 * @see log.FileHandler#Open(java.lang.String, boolean)
	 */
	@Override
	public void Open(String filePath, boolean isForce) {
		// TODO Auto-generated method stub
		try {
        	fs = new RandomAccessFile(filePath, "rwd");
        	channel = fs.getChannel();
        	mbb = channel.map(MapMode.READ_WRITE, 0, fileMaxLength);
        	this.isForce = isForce;
//        	channel = FileChannel.open(filePath, "DSYC"|"READ");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}    
	}

	/* (non-Javadoc)
	 * @see log.FileHandler#Write(java.lang.String, int, byte[])
	 */
	@Override
	public boolean Write(String filePath, int position, byte[] data) {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see log.FileHandler#Read(int, int)
	 */
	@Override
	public byte[] Read(int position, int length) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see log.FileHandler#PrepareForReadNextLine()
	 */
	@Override
	public void PrepareForReadNextLine() {
		// TODO Auto-generated method stub
		mbb.position(0);
	}

	/* (non-Javadoc)
	 * @see log.FileHandler#ReadNextObject()
	 */
	@Override
	public byte[] ReadNextObject() {
		// TODO Auto-generated method stub
		byte[] lengthBytes = new byte[4];
		mbb.get(lengthBytes);
		int length = MessageLog.toInt(lengthBytes);
//		System.out.println("length is:"+length);
		if (length == 0) {
			mbb.position(mbb.position()-4);
			return null;
		}
		
		byte[] data = new byte[length];
		mbb.get(data);
		
		return data;
	}

	/* (non-Javadoc)
	 * @see log.FileHandler#AppendBytes(byte[])
	 */
	@Override
	public boolean AppendBytes(byte[] data) {
		// TODO Auto-generated method stub
		mbb.put(data);
		if (isForce == true) {
			mbb.force();
		}
		return true;
	}

	/* (non-Javadoc)
	 * @see log.FileHandler#AppendObject(byte[])
	 */
	@Override
	public boolean AppendObject(byte[] data) {
		// TODO Auto-generated method stub
		byte[] len = MessageLog.toByteArray(data.length,4);
		byte[] res = MessageLog.byteMerger(len,data);
		
		return AppendBytes(res);
	}

	/* (non-Javadoc)
	 * @see log.FileHandler#close()
	 */
	@Override
	public void close() {
		// TODO Auto-generated method stub
		unmap(mbb);
		try {
			channel.close();
			fs.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void unmap(MappedByteBuffer buffer)
	{
		
		if (buffer == null) return;
		sun.misc.Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
		if (cleaner != null) {
			cleaner.clean();
		}
	}
}
