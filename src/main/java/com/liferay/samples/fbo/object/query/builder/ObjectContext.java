package com.liferay.samples.fbo.object.query.builder;

import com.liferay.object.model.ObjectDefinition;
import com.liferay.object.petra.sql.dsl.DynamicObjectDefinitionTable;
import com.liferay.object.system.SystemObjectDefinitionManager;
import com.liferay.petra.sql.dsl.Column;
import com.liferay.petra.sql.dsl.Table;

public class ObjectContext {

	public ObjectContext(
		ObjectDefinition objectDefinition,
		DynamicObjectDefinitionTable baseTable,
		DynamicObjectDefinitionTable extensionTable,
		SystemObjectDefinitionManager systemManager,
		Column<?, Long> primaryKeyColumn) {

		this.objectDefinition = objectDefinition;
		this.baseTable = baseTable;
		this.extensionTable = extensionTable;
		this.systemManager = systemManager;
		this.primaryKeyColumn = primaryKeyColumn;
	}

	public final ObjectDefinition objectDefinition;
	public final DynamicObjectDefinitionTable baseTable;
	public final DynamicObjectDefinitionTable extensionTable;
	public final SystemObjectDefinitionManager systemManager;
	public final Column<?, Long> primaryKeyColumn;

	public boolean isSystemObject() {
		return systemManager != null;
	}

	public Table<?> getRootTable() {
		if (systemManager != null) {
			return systemManager.getTable();
		}

		return baseTable;
	}
}
