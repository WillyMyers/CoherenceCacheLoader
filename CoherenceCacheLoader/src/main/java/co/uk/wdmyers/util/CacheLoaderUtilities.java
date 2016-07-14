package co.uk.wdmyers.util;

import java.util.Set;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.InvocationService;
import com.tangosol.net.Member;

public class CacheLoaderUtilities {
	private static final String RUNNING = "Running";
	private static final String COMPLETED = "Completed";
	private static final String FAILED = "Failed";

	@SuppressWarnings("unchecked")
	public Set<Member> getLoadingMembers(InvocationService serviceInv) {
		Set<Member> setMembers = serviceInv.getInfo().getServiceMembers();
		setMembers.remove(serviceInv.getCluster().getLocalMember());
		int members = setMembers.size();
		if (members == 0) {
			throw new IllegalStateException("No other members are running InvocationService. Is the cluster up?");
		}
		return setMembers;
	}

	public String getControlValue(String name) {
		return (String) CacheFactory.getCache("ControlCache").get(name);
	}

	public void setControlValueToRunning(String name) {
		CacheFactory.getCache("ControlCache").put(name, RUNNING);
	}

	public void setControlValueToCompleted(String name) {
		if (FAILED.equals(getControlValue(name))) {
			System.out.println("***** LOAD FAILED!! *****");
		} else {
			CacheFactory.getCache("ControlCache").put(name, COMPLETED);
		}
	}

	public void setControlValueToFailed(String name) {
		CacheFactory.getCache("ControlCache").put(name, FAILED);
	}

	public Boolean isRunning(String name) {
		if (RUNNING.equals(getControlValue(name))) {
			return Boolean.TRUE;
		}
		return Boolean.FALSE;
	}

	public Boolean shouldRun(String name) {
		String s = getControlValue(name);
		System.out.println("***** CONTROL VALUE = " + s);
		if (s == null || (!s.equals(RUNNING) && !s.equals(COMPLETED))) {
			return Boolean.TRUE;
		} else {
			return Boolean.FALSE;
		}
	}
}