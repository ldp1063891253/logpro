package com.sxt.test;

import com.sxt.etl.util.ip.IPSeeker;

public class TestIPSeeker {
	public static void main(String[] args) {
		IPSeeker ipSeeker = IPSeeker.getInstance();
		System.out.println(ipSeeker.getCountry("45.62.122.66"));
		System.out.println(ipSeeker.getCountry("192.168.100.116"));
	}
}
