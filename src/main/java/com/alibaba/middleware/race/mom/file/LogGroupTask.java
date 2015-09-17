/**
 * 
 */
package com.alibaba.middleware.race.mom.file;

import com.alibaba.middleware.race.mom.Message;

/**
 * @author showki
 *
 */
public class LogGroupTask {
	private String topic;
	private Message msg;
	private int status;
	private String[] groups;
	
	public LogGroupTask(String topic, Message msg, int status, String[] groups) {
		this.topic = topic;
		this.msg = msg;
		this.status = status;
		this.groups = groups;
	}
	
	/**
	 * @return the topic
	 */
	public String getTopic() {
		return topic;
	}
	/**
	 * @param topic the topic to set
	 */
	public void setTopic(String topic) {
		this.topic = topic;
	}
	/**
	 * @return the msg
	 */
	public Message getMsg() {
		return msg;
	}
	/**
	 * @param msg the msg to set
	 */
	public void setMsg(Message msg) {
		this.msg = msg;
	}
	/**
	 * @return the status
	 */
	public int getStatus() {
		return status;
	}
	/**
	 * @param status the status to set
	 */
	public void setStatus(int status) {
		this.status = status;
	}
	/**
	 * @return the groups
	 */
	public String[] getGroups() {
		return groups;
	}
	/**
	 * @param groups the groups to set
	 */
	public void setGroups(String[] groups) {
		this.groups = groups;
	}
}
