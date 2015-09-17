package com.alibaba.middleware.race.mom.broker.netty;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class UseTimeTool {
	
	
	//ack消息发送时间
	public static AtomicLong sendAckTime=new AtomicLong(0);
	
	//ack消息发送数量
	public static AtomicLong sendAckNumber=new AtomicLong(0);
	
	
	//消息发送时间
	public static long sendTime=0L;
	//消息发送数量
	public static long sendNumber=0L;
	
	
	//消息刷盘时间
	public static AtomicInteger flushTime=new AtomicInteger(0);
	
	//消息刷盘次数
	public static AtomicInteger flushNumber=new AtomicInteger(0);

}
