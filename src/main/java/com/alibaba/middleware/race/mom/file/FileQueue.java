/**
 * 
 */
package com.alibaba.middleware.race.mom.file;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;
import java.util.Vector;

import com.alibaba.middleware.race.mom.Message;
import com.alibaba.middleware.race.mom.model.SendTask;
import com.alibaba.middleware.race.mom.tool.Tool;

/**
 * @author showki
 *
 * attention: the method with "MultiGroupTask" can not work with those method without "MultiGroupTask"
 * 	It means that you can only use the former or only use the later.
 */
public class FileQueue {
	private int readIndex = 0;
	private int writeIndex = 0;
	private int readPosition = 0;
	private int writePosition = 0;
	private final int headLength = 4;
	private final int fileMax = 40*1024*1024;
	private String fileName = "";
	
	final private String dir = System.getProperty("user.home")+"/store/";//"$userhome/store/";
//	final private String fileSeparater = System.getProperty("file.separator");
	private FileHandler readFileHandler= null;
	private FileHandler writeFileHandler = null;
	/**
	 * @throws IOException 
	 * 
	 */
	public FileQueue(String fileName) throws IOException {
		// TODO Auto-generated constructor stub
		File fileDir = new File(dir);
		if (fileDir.exists() == false && fileDir.isDirectory() == false) {
			if (fileDir.mkdirs() == false) {
				throw new IOException("create dir error");
			}
		}
		
		this.fileName = fileName;
		String readFilePath = dir+fileName+"_queue"+readIndex;
		String writeFilePath = dir+fileName+"_queue"+writeIndex;
		System.out.println(readFilePath);
		System.out.println(writeFilePath);
		File readFile = new File(readFilePath);
		File writeFile = new File(writeFilePath);
		try {
			if (!readFile.exists()) {
				readFile.createNewFile();
			}
			if (!writeFile.exists()) {
				writeFile.createNewFile();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String abosoluteReadPath = readFile.getAbsolutePath();
		System.out.println(abosoluteReadPath);
		
		readFileHandler = (FileHandler) new FileWRFactory().getRandomAccessFileHandler();
//		synFileHandler = new FileWRFactory().getChannelFileHandler();
//		synFileHandler = new FileWRFactory().getMapChannelFileHandler();
		
		readFileHandler.Open(abosoluteReadPath, false);
		
		String abosoluteWritePath = writeFile.getAbsolutePath();
		System.out.println(abosoluteWritePath);

		writeFileHandler = (FileHandler) new FileWRFactory().getRandomAccessFileHandler();
//		asynFileHandler = new FileWRFactory().getChannelFileHandler();
//		asynFileHandler = new FileWRFactory().getMapChannelFileHandler();
		
		writeFileHandler.Open(abosoluteWritePath, false);
	}
	
	
	// data是一个LogTask序列化后的byte[]
	public boolean SynSave(byte[] data) {
		if (writePosition + data.length + headLength > fileMax) {
			GetNextFileToWrite();
		}
		if (writeFileHandler.AppendObject(data) == true) {
			writePosition += data.length + headLength;
			return true;
		} else {
			return false;
		}
	}
	
	// list里存的是每个LogTask的序列化后的byte[]。
	public boolean SynSave(List<byte[]> list) {
		byte[] temp = toByteArray(list.get(0).length, 4);
		temp = byteMerger(temp, list.get(0));
		for (int i = 1; i < list.size(); i++) {
			temp = byteMerger(temp, toByteArray(list.get(i).length, 4));
			temp = byteMerger(temp, list.get(i));
		}
		if (writePosition + temp.length > fileMax) {
			GetNextFileToWrite();
		}
		if (writeFileHandler.AppendBytes(temp) == true) {
			writePosition += temp.length;
			return true;
		} else {
			return false;
		}
	}
	
	
	private void GetNextFileToWrite() {
		writeFileHandler.close();
		
		writeIndex++;
		String newWritePath = dir+fileName+"_queue"+writeIndex;
		File writeFile = new File(newWritePath);
		if (writeFile.exists()) {
			writeFile.delete();
		}
		writePosition = 0;
		writeFileHandler.Open(newWritePath, false);
	}
	
	private void GetNextFileToRead() {
		readFileHandler.close();
		
		readIndex++;
		String newReadPath = dir+fileName+"_queue"+readIndex;
		File readFile = new File(newReadPath);
		if (!readFile.exists()) {
			System.out.println("file:"+newReadPath+" is not exist");
//			writeFile.delete();
		}
		readPosition = 0;
		readFileHandler.Open(newReadPath, false);
	}
	
	public List<SendTask> Restore(int number) {
		List<SendTask> res = new Vector<SendTask>();
		int restoredNum = 0;
		while (restoredNum < number) {
			if (readIndex > writeIndex || (readIndex == writeIndex && readPosition >= writePosition)) 
				break;
			
			byte[] temp = readFileHandler.ReadNextObject();
			if (temp == null) {	
				// read nothing, because reach current file' end or reach all file'end
				if (readIndex < writeIndex) {
					GetNextFileToRead();
				} else {
					break;
				}
			} else {
				readPosition += temp.length + headLength;
				LogTask logTask = Tool.deserialize(temp, LogTask.class);
				SendTask sendTask = logTask.getTask();
				res.add(sendTask);
				restoredNum++;
			}
		}
		return res;
	}
	
////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////
	
	// 用于写入从client端返回的ack日志
	public boolean AsynSaveMultiGroupTask(String topic, Message msg, int status, String group) {
		SendTask task=new SendTask();
		task.setGroupId(group);
		task.setMessage(msg);
		task.setTopic(topic);
		LogTask logTask = new LogTask(task, status);
		return AsynSaveMultiGroupTask(logTask);
	}
	
	// 用于写入从client端返回的ack日志
	public boolean AsynSaveMultiGroupTask(LogTask logTask) {
		String[] groupStrings = new String[1]; 
		groupStrings[0] = logTask.getTask().getGroupId();
		LogGroupTask logGroupTask = new LogGroupTask(
				logTask.getTask().getTopic(), 
				logTask.getTask().getMessage(), 
				logTask.getStatus(), 
				groupStrings);

		return AsynSaveMultiGroupTask(logGroupTask);
	}
	
	public boolean AsynSaveMultiGroupTask(String topic, Message msg, int status, String[] groups) {
		LogGroupTask logGroupTask = new LogGroupTask(topic, msg, status, groups);
		return AsynSaveMultiGroupTask(logGroupTask);
	}
	
	public boolean AsynSaveMultiGroupTask(LogGroupTask logGroupTask) {
		byte[] res = Tool.serialize(logGroupTask);
		if (writePosition + res.length + headLength> fileMax) {
			GetNextFileToWrite();
		}
		if (writeFileHandler.AppendObject(res) == true) {
			writePosition += res.length + headLength;
			return true;
		} else {
			return false;
		}
	}


	public boolean AsynSaveMultiGroupTask(List<LogGroupTask> list) {
		byte[] res = GenerateBytesFromMultiGroupTask(list);
		if (writePosition + res.length > fileMax) {
			GetNextFileToWrite();
		}
		if (writeFileHandler.AppendBytes(res) == true) {
			writePosition += res.length;
			return true;
		} else {
			return false;
		}
	}

	private byte[] GenerateBytesFromMultiGroupTask(List<LogGroupTask> list) {
		byte[] temp = Tool.serialize(list.get(0));
		byte[] res = byteMerger(toByteArray(temp.length, 4), temp);
		for (int i = 1; i < list.size(); i++) {
			temp = Tool.serialize(list.get(i));
			res  = byteMerger(res, toByteArray(temp.length, 4));
			res = byteMerger(res, temp);
		}
		return res;
	}

	// the num means the number of LogGroupTask to be restored
	public List<SendTask> RestoreMultiGroupTask(int num) {
		List<SendTask> res = new Vector<SendTask>();
		int restoredNum = 0;
		while (restoredNum < num) {
			if (readIndex > writeIndex || (readIndex == writeIndex && readPosition >= writePosition)) 
				break;
			
			byte[] temp = readFileHandler.ReadNextObject();
			if (temp == null) {	
				// read nothing, because reach current file' end or reach all file'end
				if (readIndex < writeIndex) {
					GetNextFileToRead();
				} else {
					break;
				}
			} else {
				readPosition += temp.length + headLength;
				
				LogGroupTask logGroupTask = Tool.deserialize(temp, LogGroupTask.class);
				String topic = logGroupTask.getTopic();
				Message msg = logGroupTask.getMsg();
				int status = logGroupTask.getStatus(); 	// ignore status
				String[] groupStrings = logGroupTask.getGroups();
				SendTask task=null;
				for (String group : groupStrings) {
					task=new SendTask();
					task.setGroupId(group);
					task.setMessage(msg);
					task.setTopic(topic);
					res.add(task);
				}
				restoredNum++;
			}
		}
		return res;
	}
	
	/*
	* @param original of type byte[] 原数组
	* @param from 起始点
	* @param to 结束点
	* @return 返回copy的数组
	*/
	public static byte[] copyOfRange(byte[] original, int from, int length) {
		if (length < 0)
			throw new IllegalArgumentException("lenght < 0");
		if (from + length > original.length)
			throw new IllegalArgumentException("length is too long");
		byte[] copy = new byte[length];
		System.arraycopy(original, from, copy, 0, length);
		return copy;
	}

	
	static byte[] byteMerger(byte[] byte_1, byte[] byte_2){  
        byte[] byte_3 = new byte[byte_1.length+byte_2.length];  
        System.arraycopy(byte_1, 0, byte_3, 0, byte_1.length);  
        System.arraycopy(byte_2, 0, byte_3, byte_1.length, byte_2.length);  
        return byte_3;  
    }
	static byte[] toByteArray(int iSource, int iArrayLen) {
	    byte[] bLocalArr = new byte[iArrayLen];
	    for (int i = 0; (i < 4) && (i < iArrayLen); i++) {
	        bLocalArr[i] = (byte) (iSource >> 8 * i & 0xFF);
	    }
	    return bLocalArr;
	}

	// 将byte数组bRefArr转为一个整数,字节数组的低位是整型的低字节位
	static int toInt(byte[] bRefArr) {
	    int iOutcome = 0;
	    byte bLoop;

	    for (int i = 0; i < bRefArr.length; i++) {
	        bLoop = bRefArr[i];
	        iOutcome += (bLoop & 0xFF) << (8 * i);
	    }
	    return iOutcome;
	}

}
