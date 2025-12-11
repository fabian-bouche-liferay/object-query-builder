package com.liferay.samples.fbo.object.query.builder;

import com.liferay.object.model.ObjectDefinition;
import com.liferay.object.service.ObjectDefinitionLocalService;
import com.liferay.object.service.ObjectFieldLocalService;
import com.liferay.object.service.ObjectRelationshipLocalService;
import com.liferay.portal.kernel.exception.PortalException;

public class CompanyScopedBuilder {

	private final long _companyId;
	private final ObjectDslQueryBuilder _builder;

	public CompanyScopedBuilder(long companyId, ObjectDslQueryBuilder builder,
			ObjectDefinitionLocalService objectDefinitionLocalService,
			ObjectFieldLocalService objectFieldLocalService,
			ObjectRelationshipLocalService objectRelationshipLocalService) {
		_companyId = companyId;
		_builder = builder;
		_objectDefinitionLocalService = objectDefinitionLocalService;
		_objectFieldLocalService = objectFieldLocalService;
		_objectRelationshipLocalService = objectRelationshipLocalService;
	}
	
	public ObjectDslQuery forDefinition(String objectDefinitionERC)
		throws PortalException {

		ObjectDefinition objectDefinition =
			_objectDefinitionLocalService.getObjectDefinitionByExternalReferenceCode(
				objectDefinitionERC, _companyId);

		ObjectContext context = _builder.createContext(objectDefinition);

		return new ObjectDslQuery(
			context, _objectDefinitionLocalService,
			_objectFieldLocalService,
			_objectRelationshipLocalService);
	}

	public ObjectDslQuery forDefinition(long objectDefinitionId)
		throws PortalException {

		ObjectDefinition objectDefinition =
				_objectDefinitionLocalService.getObjectDefinition(
				objectDefinitionId);

		ObjectContext context = _builder.createContext(objectDefinition);

		return new ObjectDslQuery(
			context, _objectDefinitionLocalService,
			_objectFieldLocalService,
			_objectRelationshipLocalService);
	}
	
	private final ObjectDefinitionLocalService _objectDefinitionLocalService;
	private final ObjectFieldLocalService _objectFieldLocalService;
	private final ObjectRelationshipLocalService
		_objectRelationshipLocalService;
}