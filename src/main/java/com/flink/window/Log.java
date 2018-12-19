package com.flink.window;

public class Log {

	private String id;
	private long time;
	private int uv;
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public long getTime() {
		return time;
	}
	public void setTime(long time) {
		this.time = time;
	}
	public int getUv() {
		return uv;
	}
	public void setUv(int uv) {
		this.uv = uv;
	}
	@Override
	public String toString() {
		return "Log [id=" + id + ", time=" + time + ", uv=" + uv + "]";
	}
	
	
}
