package com.liferay.samples.fbo.object.query.builder;

import com.liferay.object.model.ObjectDefinition;
import com.liferay.object.petra.sql.dsl.DynamicObjectDefinitionTable;
import com.liferay.object.service.ObjectDefinitionLocalService;
import com.liferay.object.service.ObjectFieldLocalService;
import com.liferay.object.service.ObjectRelationshipLocalService;
import com.liferay.object.system.SystemObjectDefinitionManager;
import com.liferay.object.system.SystemObjectDefinitionManagerRegistry;
import com.liferay.portal.kernel.exception.PortalException;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;

@Component(
		immediate = true,
		service = ObjectDslQueryBuilder.class
)
public class ObjectDslQueryBuilder {

	public CompanyScopedBuilder forCompany(long companyId) {
		return new CompanyScopedBuilder(companyId, this, 
				_objectDefinitionLocalService,
				_objectFieldLocalService,
				_objectRelationshipLocalService);
	}

	public ObjectDslQuery forDefinition(
			long companyId, String objectDefinitionERC)
		throws PortalException {

		ObjectDefinition objectDefinition =
			_objectDefinitionLocalService.
				getObjectDefinitionByExternalReferenceCode(
					objectDefinitionERC, companyId);

		ObjectContext context = createContext(objectDefinition);

		return new ObjectDslQuery(
			context, _objectDefinitionLocalService,
			_objectFieldLocalService, _objectRelationshipLocalService);
	}

	public ObjectDslQuery forDefinition(long objectDefinitionId)
		throws PortalException {

		ObjectDefinition objectDefinition =
			_objectDefinitionLocalService.getObjectDefinition(
				objectDefinitionId);

		ObjectContext context = createContext(objectDefinition);

		return new ObjectDslQuery(
			context, _objectDefinitionLocalService,
			_objectFieldLocalService, _objectRelationshipLocalService);
	}

	public ObjectContext getContext(long objectDefinitionId)
		throws PortalException {

		ObjectDefinition objectDefinition =
			_objectDefinitionLocalService.getObjectDefinition(
				objectDefinitionId);

		return createContext(objectDefinition);
	}

	ObjectContext createContext(ObjectDefinition objectDefinition)
		throws PortalException {

		DynamicObjectDefinitionTable baseTable =
			_createDynamicTable(objectDefinition);

		DynamicObjectDefinitionTable extensionTable =
			_createExtensionDynamicTable(objectDefinition);

		SystemObjectDefinitionManager systemManager = null;
		com.liferay.petra.sql.dsl.Column<?, Long> pkColumn;

		if (objectDefinition.isUnmodifiableSystemObject()) {
			systemManager =
				_systemObjectDefinitionManagerRegistry.
					getSystemObjectDefinitionManager(
						objectDefinition.getName());

			pkColumn = systemManager.getPrimaryKeyColumn();
		}
		else {
			pkColumn = baseTable.getPrimaryKeyColumn();
		}

		return new ObjectContext(
			objectDefinition, baseTable, extensionTable, systemManager,
			pkColumn);
	}

	private DynamicObjectDefinitionTable _createDynamicTable(
			ObjectDefinition objectDefinition)
		throws PortalException {

		long objectDefinitionId =
			objectDefinition.getObjectDefinitionId();

		return new DynamicObjectDefinitionTable(
			objectDefinition,
			_objectFieldLocalService.getObjectFields(
				objectDefinitionId, objectDefinition.getDBTableName()),
			objectDefinition.getDBTableName());
	}

	private DynamicObjectDefinitionTable _createExtensionDynamicTable(
			ObjectDefinition objectDefinition)
		throws PortalException {

		long objectDefinitionId =
			objectDefinition.getObjectDefinitionId();

		return new DynamicObjectDefinitionTable(
			objectDefinition,
			_objectFieldLocalService.getObjectFields(
				objectDefinitionId,
				objectDefinition.getExtensionDBTableName()),
			objectDefinition.getExtensionDBTableName());
	}

	@Reference
	private ObjectDefinitionLocalService _objectDefinitionLocalService;

	@Reference
	private ObjectFieldLocalService _objectFieldLocalService;

	@Reference
	private ObjectRelationshipLocalService _objectRelationshipLocalService;

	@Reference
	private SystemObjectDefinitionManagerRegistry
		_systemObjectDefinitionManagerRegistry;
}
