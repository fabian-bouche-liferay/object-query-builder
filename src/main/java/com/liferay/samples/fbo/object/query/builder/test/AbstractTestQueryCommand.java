package com.liferay.samples.fbo.object.query.builder.test;

import com.liferay.portal.kernel.exception.PortalException;
import com.liferay.portal.kernel.transaction.Propagation;
import com.liferay.portal.kernel.transaction.TransactionConfig;
import com.liferay.portal.kernel.transaction.TransactionInvokerUtil;

public abstract class AbstractTestQueryCommand {

	protected static final TransactionConfig _transactionConfig =
		TransactionConfig.Factory.create(
			Propagation.REQUIRED,
			new Class<?>[] {Exception.class}
		);

	public void testQuery(long companyId) throws Throwable {
		TransactionInvokerUtil.invoke(
			_transactionConfig,
			() -> {
				_doTestQuery(companyId);
				return null;
			}
		);
	}

	protected abstract void _doTestQuery(long companyId) throws PortalException;

}
