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
 */
public  class MessageLog {
	final private String dir = System.getProperty("user.home")+"/store/";//"$userhome/store/";
//	final private String fileSeparater = System.getProperty("file.separator");
	private FileHandler synFileHandler= null;
	private FileHandler asynFileHandler = null;
	/**
	 * @throws IOException 
	 * 
	 */
	public MessageLog(String fileName) throws IOException {
		// TODO Auto-generated constructor stub
		File fileDir = new File(dir);
		if (fileDir.exists() == false && fileDir.isDirectory() == false) {
			if (fileDir.mkdirs() == false) {
				throw new IOException("create dir error");
			}
		}
		
		String fullSynPath = dir+fileName+"_syn";
		String fullAsynPath = dir+fileName+"_asyn";
		System.out.println(fullSynPath);
		System.out.println(fullAsynPath);
		File synFile = new File(fullSynPath);
		File asynFile = new File(fullAsynPath);
		try {
			if (!synFile.exists()) {
				synFile.createNewFile();
			}
			if (!asynFile.exists()) {
				asynFile.createNewFile();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String abosoluteSynPath = synFile.getAbsolutePath();
		System.out.println(abosoluteSynPath);
		
		synFileHandler = (FileHandler) new FileWRFactory().getRandomAccessFileHandler();
//		synFileHandler = new FileWRFactory().getChannelFileHandler();
//		synFileHandler = new FileWRFactory().getMapChannelFileHandler();
		
		synFileHandler.Open(abosoluteSynPath, true);
		
		String abosoluteAsynPath = asynFile.getAbsolutePath();
		System.out.println(abosoluteAsynPath);

		asynFileHandler = (FileHandler) new FileWRFactory().getRandomAccessFileHandler();
//		asynFileHandler = new FileWRFactory().getChannelFileHandler();
//		asynFileHandler = new FileWRFactory().getMapChannelFileHandler();
		
		asynFileHandler.Open(abosoluteAsynPath, false);
	}
	
	public boolean AsynSave(byte[] data) {
		return asynFileHandler.AppendObject(data);
	}
	
	// list里存的是每个LogTask的序列化后的byte[]。
	public boolean SynSave(List<byte[]> list) {
		byte[] temp = toByteArray(list.get(0).length, 4);
		temp = byteMerger(temp, list.get(0));
		for (int i = 1; i < list.size(); i++) {
			temp = byteMerger(temp, toByteArray(list.get(i).length, 4));
			temp = byteMerger(temp, list.get(i));
		}
		return synFileHandler.AppendBytes(temp);
	}
	
	// data是一个LogTask序列化后的byte[]
	public boolean SynSave(byte[] data) {
		return synFileHandler.AppendObject(data);
	}
	
	
	public List<SendTask> Restore() throws IOException, ClassNotFoundException {
		List<SendTask> res = new Vector<SendTask>();
		synFileHandler.PrepareForReadNextLine();
		byte[] temp = null;
		while((temp = synFileHandler.ReadNextObject()) != null) {
			ByteArrayInputStream is = new ByteArrayInputStream(temp);
			ObjectInputStream in = new ObjectInputStream(is);
			LogTask task = (LogTask) in.readObject();
			res.add(task.getTask());
		}
		
		asynFileHandler.PrepareForReadNextLine();
		while ((temp = asynFileHandler.ReadNextObject()) != null) {
			ByteArrayInputStream is = new ByteArrayInputStream(temp);
			ObjectInputStream in = new ObjectInputStream(is);
			LogTask task = (LogTask) in.readObject();
			boolean isOk = res.remove(task.getTask());
			if (isOk == false) {
//				System.out.println("something error, don't contain this object");
			}
		}
		return res;
	}
	
//	public boolean AsynSaveMultiGroupTask(String topic, Message msg, int status, String[] groups) {
//		byte[] res = GenerateBytesFromMultiGroupTask(topic, msg, status, groups);
//		return asynFileHandler.AppendBytes(res);
//	}
//	
//	public boolean SynSaveMultiGroupTask(String topic, Message msg, int status, String[] groups) {
//		byte[] res = GenerateBytesFromMultiGroupTask(topic, msg, status, groups);
//		return synFileHandler.AppendBytes(res);
//	}
	
	// 用于写入从client端返回的ack日志
	public boolean AsynSaveMultiGroupTask(LogTask logTask) {
		String[] groupStrings = new String[1]; 
		groupStrings[0] = logTask.getTask().getGroupId();
//		return AsynSaveMultiGroupTask(
//				logTask.getTask().getTopic(), 
//				logTask.getTask().getMessage(), 
//				logTask.getStatus(), 
//				groupStrings);
		
		LogGroupTask logGroupTask = new LogGroupTask(
				logTask.getTask().getTopic(), 
				logTask.getTask().getMessage(), 
				logTask.getStatus(), 
				groupStrings);
		return asynFileHandler.AppendObject(Tool.serialize(logGroupTask));
	}
	
	// 用于写入从client端返回的ack日志
	public boolean AsynSaveMultiGroupTask(String topic, Message msg, int status, String group) {
		String[] groupStrings = new String[1]; 
		groupStrings[0] = group;
//		return AsynSaveMultiGroupTask(topic,msg,status,groupStrings);
		LogGroupTask logGroupTask = new LogGroupTask(topic, msg, status, groupStrings);
		return asynFileHandler.AppendObject(Tool.serialize(logGroupTask));
		
	}

	
	public boolean AsynSaveMultiGroupTask(List<LogGroupTask> list) {
		byte[] res = GenerateBytesFromMultiGroupTask(list);
		return asynFileHandler.AppendBytes(res);
//		return asynFileHandler.AppendObject(res);
	}
	
	public boolean SynSaveMultiGroupTask(List<LogGroupTask> list) {
		byte[] res = GenerateBytesFromMultiGroupTask(list);
		return synFileHandler.AppendBytes(res);
//		return synFileHandler.AppendObject(res);
	}
	
	
	private byte[] GenerateBytesFromMultiGroupTask(LogGroupTask task) {
		return Tool.serialize(task);
	}
	
	// 输入同一个msg的信息和对应的group组信息，拼成如下格式：
	// TotalLength|msgLen|msg|topicLen|topic|status|groupNum|length1,group..length2,group..length3,group..
	private byte[] GenerateBytesFromMultiGroupTask(String topic, Message msg, int status, String[] groups) {
		
		byte[] messageBytes = Tool.serialize(msg);
		if (messageBytes == null) {
			System.out.println("serialize message failed");
			return null;
		}
		byte[] messageInfo = byteMerger(toByteArray(messageBytes.length, 4), messageBytes);
		
		byte[] topicBytes = topic.getBytes();
		byte[] topicInfo = byteMerger(toByteArray(topicBytes.length, 4), topicBytes);
		
		byte[] other = byteMerger(messageInfo, topicInfo);
		other = byteMerger(other, toByteArray(status, 4));
		
		int groupNum = groups.length;
		other = byteMerger(other, toByteArray(groupNum, 4));
		for (int i = 0; i < groups.length; i++) {
			String string = groups[i];
			byte[] group= string.getBytes();
			int len = group.length;
			other = byteMerger(other, toByteArray(len, 4));
			other = byteMerger(other, group);
		}
		return byteMerger(toByteArray(other.length,4), other);
	}
	
	private byte[] GenerateBytesFromMultiGroupTask(List<LogGroupTask> list) {
		byte[] temp = Tool.serialize(list.get(0));
//				GenerateBytesFromMultiGroupTask(
//				logGroupTask.getTopic(), 
//				logGroupTask.getMsg(), 
//				logGroupTask.getStatus(), 
//				logGroupTask.getGroups());
		byte[] res = byteMerger(toByteArray(temp.length, 4), temp);
		for (int i = 1; i < list.size(); i++) {
			temp = Tool.serialize(list.get(i));
			res  = byteMerger(res, toByteArray(temp.length, 4));
			res = byteMerger(res, temp);
//			res = byteMerger(res, GenerateBytesFromMultiGroupTask(
//					logGroupTask.getTopic(), 
//					logGroupTask.getMsg(), 
//					logGroupTask.getStatus(), 
//					logGroupTask.getGroups()));
		}
		return res;
	}
	
	// TotalLength|msgLen|msg|topicLen|topic|groupNum|length1,group..length2,group..length3,group..
	public List<SendTask> RestoreAllMultiGroupTask() {
		List<SendTask> res = new Vector<SendTask>();
		synFileHandler.PrepareForReadNextLine();
		byte[] temp = null;
		
		long sTime = 0;
		long eTime = 0;
		long startTime = 0;
		long endTime = 0;
		
		long totalTime = 0;
		sTime = System.currentTimeMillis();
		while((temp = synFileHandler.ReadNextObject()) != null) {
			startTime = System.currentTimeMillis();
			res.addAll(RestoreMultiGroupTask(temp));
			endTime = System.currentTimeMillis();
			totalTime += endTime - startTime;
		}
		System.out.println("syn---transform byte[] to SendTask use:"+ totalTime+" ms");
		System.out.println("syn---read byte[] from file use:"+(System.currentTimeMillis()-sTime-totalTime)+" ms");
		
		totalTime = 0;
		sTime = System.currentTimeMillis();
		asynFileHandler.PrepareForReadNextLine();
		while((temp = asynFileHandler.ReadNextObject()) != null) {
			List<SendTask> list = RestoreMultiGroupTask(temp);	
			startTime = System.currentTimeMillis();
			res.removeAll(list);
			endTime = System.currentTimeMillis();
			totalTime += endTime - startTime;
		}
		System.out.println("asyn---transform byte[] to SendTask use:"+ totalTime+" ms");
		System.out.println("asyn---read byte[] from file use:"+(System.currentTimeMillis()-sTime-totalTime)+" ms");

		return res;
	}
	
	private List<SendTask> RestoreMultiGroupTask(byte[] temp) {

		long startTime2 = System.currentTimeMillis();
		List<SendTask> res = new Vector<SendTask>();
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
		return res;
	}
	
	private List<SendTask> OldRestoreMultiGroupTask(byte[] temp) {
		long startTime1 = 0, startTime2 = 0;
		long endTime1 = 0, endTime2 = 0;
		
//		startTime1 = System.currentTimeMillis();
		List<SendTask> res = new Vector<SendTask>();
		int position = 0;
		int msgLen = toInt(copyOfRange(temp, position, 4));
		position += 4;
		startTime2 = System.currentTimeMillis();
		Message msg = Tool.deserialize(copyOfRange(temp, position, msgLen), Message.class);
//		System.out.println("deserialize use:"+(System.currentTimeMillis()-startTime2) + " ms");
		if (msg == null) {
			System.out.println("deserialize message failed");
			return null;
		}
		position += msgLen;
		
		int topicLen = toInt(copyOfRange(temp, position, 4));
		position += 4;
		String topic = new String(copyOfRange(temp, position, topicLen));
		position += topicLen;
		
		// ignore status
		position += 4;
		
		int groupNum = toInt(copyOfRange(temp, position, 4));
		position += 4;
		SendTask task=null;
		for (int i = 0; i < groupNum; i++) {
			int groupLen = toInt(copyOfRange(temp, position, 4));
			position += 4;
			String group = new String(copyOfRange(temp, position, groupLen));
			position += groupLen;
			task=new SendTask();
			task.setGroupId(group);
			task.setMessage(msg);
			task.setTopic(topic);
			res.add(task);
		}
//		System.out.println("restore a GroupSendTask use:" + (System.currentTimeMillis() - startTime1) + " ms");
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
