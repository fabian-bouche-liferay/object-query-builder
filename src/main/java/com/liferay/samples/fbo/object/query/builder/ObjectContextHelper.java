package com.liferay.samples.fbo.object.query.builder;

import com.liferay.object.model.ObjectDefinition;
import com.liferay.object.petra.sql.dsl.DynamicObjectDefinitionTable;
import com.liferay.object.service.ObjectFieldLocalService;
import com.liferay.object.system.SystemObjectDefinitionManager;
import com.liferay.object.system.SystemObjectDefinitionManagerRegistry;
import com.liferay.portal.kernel.exception.PortalException;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;

@Component(service = ObjectContextHelper.class)
public class ObjectContextHelper {

	public ObjectContext createContext(ObjectDefinition objectDefinition)
		throws PortalException {

		DynamicObjectDefinitionTable baseTable =
			new DynamicObjectDefinitionTable(
				objectDefinition,
				_objectFieldLocalService.getObjectFields(
					objectDefinition.getObjectDefinitionId(),
					objectDefinition.getDBTableName()),
				objectDefinition.getDBTableName());

		DynamicObjectDefinitionTable extensionTable =
			new DynamicObjectDefinitionTable(
				objectDefinition,
				_objectFieldLocalService.getObjectFields(
					objectDefinition.getObjectDefinitionId(),
					objectDefinition.getExtensionDBTableName()),
				objectDefinition.getExtensionDBTableName());

		SystemObjectDefinitionManager systemManager = null;
		com.liferay.petra.sql.dsl.Column<?, Long> pkColumn;

		if (objectDefinition.isUnmodifiableSystemObject()) {
			systemManager =
				_systemObjectDefinitionManagerRegistry.getSystemObjectDefinitionManager(
					objectDefinition.getName());

			pkColumn = systemManager.getPrimaryKeyColumn();
		}
		else {
			pkColumn = baseTable.getPrimaryKeyColumn();
		}

		return new ObjectContext(
			objectDefinition, baseTable, extensionTable, systemManager, pkColumn);
	}

	@Reference
	private ObjectFieldLocalService _objectFieldLocalService;

	@Reference
	private SystemObjectDefinitionManagerRegistry _systemObjectDefinitionManagerRegistry;

}
