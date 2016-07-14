package co.uk.wdmyers.load;

import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.InvocationObserver;
import com.tangosol.net.Member;

import co.uk.wdmyers.util.CacheLoaderUtilities;

public class CacheLoaderObserver implements InvocationObserver {

	final Logger log = LoggerFactory.getLogger(CacheLoaderObserver.class);
	public String cacheName;
	private CacheLoaderUtilities utils = new CacheLoaderUtilities();
	public CountDownLatch latch;
	
	public CacheLoaderObserver(String cacheName, CountDownLatch latch) {
		this.cacheName = cacheName;
		this.latch = latch;
	}

	@Override
	public void memberCompleted(Member member, Object oResult) {
		latch.countDown();
		log.info(String.format("%s: load finished on %s", cacheName, member.toString()));
	}

	@Override
	public void memberFailed(Member member, Throwable eFailure) {
		latch.countDown();
		log.error(String.format("%s: load failed on %s", cacheName, member.toString()));
		CacheFactory.log(eFailure);
		utils.setControlValueToFailed(cacheName);
	}

	@Override
	public void memberLeft(Member member) { // TODO: resubmit to a member that									// is up
		latch.countDown();
		log.error(String.format("%s: member left before load finished (%s)", cacheName, member.toString()));
	}

	@Override
	public void invocationCompleted() {
		log.info(String.format("%s: invocation has completed", cacheName));
	}

}