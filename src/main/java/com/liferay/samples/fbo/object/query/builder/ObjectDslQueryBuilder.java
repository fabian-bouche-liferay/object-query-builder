package com.liferay.samples.fbo.object.query.builder;

import com.liferay.object.model.ObjectDefinition;
import com.liferay.object.service.ObjectDefinitionLocalService;
import com.liferay.object.service.ObjectFieldLocalService;
import com.liferay.object.service.ObjectRelationshipLocalService;
import com.liferay.portal.kernel.exception.PortalException;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;

@Component(
		immediate = true,
		service = ObjectDslQueryBuilder.class
)
public class ObjectDslQueryBuilder {

	public CompanyScopedBuilder forCompany(long companyId) {
		return new CompanyScopedBuilder(companyId,
				_objectDefinitionLocalService,
				_objectFieldLocalService,
				_objectRelationshipLocalService,
				_objectContextHelper);
	}
	
	public ObjectDslQuery forDefinition(long companyId, String erc)
		throws PortalException {

		ObjectDefinition od =
			_objectDefinitionLocalService.getObjectDefinitionByExternalReferenceCode(
				erc, companyId);

		ObjectContext ctx = _objectContextHelper.createContext(od);

		return new ObjectDslQuery(ctx, _objectContextHelper,
			_objectDefinitionLocalService, _objectFieldLocalService, _objectRelationshipLocalService);
	}

	@Reference
	private ObjectContextHelper _objectContextHelper;

	@Reference
	private ObjectDefinitionLocalService _objectDefinitionLocalService;

	@Reference
	private ObjectFieldLocalService _objectFieldLocalService;

	@Reference
	private ObjectRelationshipLocalService _objectRelationshipLocalService;
}
