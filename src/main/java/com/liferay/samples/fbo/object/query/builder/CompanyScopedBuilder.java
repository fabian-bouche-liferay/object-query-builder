package com.liferay.samples.fbo.object.query.builder;

import com.liferay.object.model.ObjectDefinition;
import com.liferay.object.service.ObjectDefinitionLocalService;
import com.liferay.object.service.ObjectFieldLocalService;
import com.liferay.object.service.ObjectRelationshipLocalService;
import com.liferay.portal.kernel.exception.PortalException;

public class CompanyScopedBuilder {

	private final long _companyId;

	public CompanyScopedBuilder(long companyId,
			ObjectDefinitionLocalService objectDefinitionLocalService,
			ObjectFieldLocalService objectFieldLocalService,
			ObjectRelationshipLocalService objectRelationshipLocalService,
			ObjectContextHelper objectContextHelper) {
		_companyId = companyId;
		_objectDefinitionLocalService = objectDefinitionLocalService;
		_objectFieldLocalService = objectFieldLocalService;
		_objectRelationshipLocalService = objectRelationshipLocalService;
		_objectContextHelper = objectContextHelper;
	}
	
	public ObjectDslQuery forDefinition(String objectDefinitionERC)
		throws PortalException {

		ObjectDefinition objectDefinition =
			_objectDefinitionLocalService.getObjectDefinitionByExternalReferenceCode(
				objectDefinitionERC, _companyId);

		ObjectContext context = _objectContextHelper.createContext(objectDefinition);
		
		return new ObjectDslQuery(context, _objectContextHelper,
				_objectDefinitionLocalService, _objectFieldLocalService, _objectRelationshipLocalService);
	}

	public ObjectDslQuery forDefinition(long objectDefinitionId)
		throws PortalException {

		ObjectDefinition objectDefinition =
				_objectDefinitionLocalService.getObjectDefinition(
				objectDefinitionId);

		ObjectContext context = _objectContextHelper.createContext(objectDefinition);

		return new ObjectDslQuery(context, _objectContextHelper,
				_objectDefinitionLocalService, _objectFieldLocalService, _objectRelationshipLocalService);
	}
	
	private final ObjectDefinitionLocalService _objectDefinitionLocalService;
	private final ObjectFieldLocalService _objectFieldLocalService;
	private final ObjectRelationshipLocalService
		_objectRelationshipLocalService;
	private final ObjectContextHelper _objectContextHelper;
}