package com.liferay.samples.fbo.object.query.builder;

import com.liferay.object.constants.ObjectRelationshipConstants;
import com.liferay.object.model.ObjectDefinition;
import com.liferay.object.model.ObjectField;
import com.liferay.object.model.ObjectRelationship;
import com.liferay.object.petra.sql.dsl.DynamicObjectDefinitionTable;
import com.liferay.object.petra.sql.dsl.DynamicObjectRelationshipMappingTable;
import com.liferay.object.service.ObjectDefinitionLocalService;
import com.liferay.object.service.ObjectFieldLocalService;
import com.liferay.object.service.ObjectRelationshipLocalService;
import com.liferay.petra.sql.dsl.Column;
import com.liferay.petra.sql.dsl.DSLQueryFactoryUtil;
import com.liferay.petra.sql.dsl.expression.Expression;
import com.liferay.petra.sql.dsl.expression.Predicate;
import com.liferay.petra.sql.dsl.query.DSLQuery;
import com.liferay.petra.sql.dsl.query.JoinStep;
import com.liferay.portal.kernel.exception.PortalException;

import java.io.Serializable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ObjectDslQuery {

	private final ObjectContext _ctx;

	private final ObjectDefinitionLocalService _objectDefinitionLocalService;
	private final ObjectFieldLocalService _objectFieldLocalService;
	private final ObjectRelationshipLocalService
		_objectRelationshipLocalService;

	private final List<Expression<?>> _selectExpressions =
		new ArrayList<>();

	private JoinStep _joinStep;
	private Predicate _predicate;

	public ObjectDslQuery(
		ObjectContext ctx,
		ObjectDefinitionLocalService objectDefinitionLocalService,
		ObjectFieldLocalService objectFieldLocalService,
		ObjectRelationshipLocalService objectRelationshipLocalService) {

		_ctx = ctx;
		_objectDefinitionLocalService = objectDefinitionLocalService;
		_objectFieldLocalService = objectFieldLocalService;
		_objectRelationshipLocalService = objectRelationshipLocalService;
	}

	public ObjectDslQuery select(Expression<?>... expressions) {
		for (Expression<?> expression : expressions) {
			_selectExpressions.add(expression);
		}

		return this;
	}

	public ObjectDslQuery selectPrimaryKey() {
		_selectExpressions.add(_ctx.primaryKeyColumn);

		return this;
	}

	public ObjectDslQuery fromBase() {
		if (_joinStep != null) {
			return this;
		}

		if (_selectExpressions.isEmpty()) {
			_selectExpressions.add(_ctx.primaryKeyColumn);
		}

		Expression<?>[] selectArray = _selectExpressions.toArray(
			new Expression<?>[0]);

		if (_ctx.isSystemObject()) {
			_joinStep = DSLQueryFactoryUtil.select(
				selectArray
			).from(
				_ctx.getRootTable()
			).leftJoinOn(
				_ctx.extensionTable,
				_ctx.systemManager.getPrimaryKeyColumn(
				).eq(
					_ctx.extensionTable.getPrimaryKeyColumn()
				)
			);
		}
		else {
			_joinStep = DSLQueryFactoryUtil.select(
				selectArray
			).from(
				_ctx.baseTable
			).leftJoinOn(
				_ctx.extensionTable,
				_ctx.baseTable.getPrimaryKeyColumn(
				).eq(
					_ctx.extensionTable.getPrimaryKeyColumn()
				)
			);
		}

		return this;
	}

	public ObjectDslQuery joinRelationship(long objectRelationshipId)
		throws PortalException {

		ObjectRelationship objectRelationship =
			_objectRelationshipLocalService.getObjectRelationship(
				objectRelationshipId);

		return _joinRelationship(objectRelationship);
	}

	public ObjectDslQuery joinRelationship(String relationshipName)
		throws PortalException {

		List<ObjectRelationship> relationships =
			_objectRelationshipLocalService.getObjectRelationships(
				_ctx.objectDefinition.getObjectDefinitionId());

		for (ObjectRelationship relationship : relationships) {
			if (Objects.equals(relationship.getName(), relationshipName)) {
				return _joinRelationship(relationship);
			}
		}

		throw new PortalException(
			"ObjectRelationship not found for name=" + relationshipName +
				" and objectDefinitionId=" +
					_ctx.objectDefinition.getObjectDefinitionId());
	}

	public ObjectDslQuery where(Predicate predicate) {
		_predicate = predicate;

		return this;
	}

	public ObjectDslQuery and(Predicate predicate) {
		if (_predicate == null) {
			_predicate = predicate;
		}
		else {
			_predicate = _predicate.and(predicate);
		}

		return this;
	}

	public ObjectDslQuery whereRelatedFieldEquals(
			long relatedObjectDefinitionId, String fieldName,
			Serializable value)
		throws PortalException {

		Predicate predicate = _eqRelatedField(
			relatedObjectDefinitionId, fieldName, value);

		return and(predicate);
	}

	public ObjectDslQuery whereRelatedFieldEquals(
			String relatedObjectDefinitionERC, String fieldName,
			Serializable value)
		throws PortalException {

		long companyId = _ctx.objectDefinition.getCompanyId();

		ObjectDefinition relatedObjectDefinition =
			_objectDefinitionLocalService.
				getObjectDefinitionByExternalReferenceCode(
					relatedObjectDefinitionERC, companyId);

		return whereRelatedFieldEquals(
			relatedObjectDefinition.getObjectDefinitionId(), fieldName,
			value);
	}

	public DSLQuery build() {
		if (_joinStep == null) {
			fromBase();
		}

		if (_predicate != null) {
			return _joinStep.where(_predicate);
		}

		return _joinStep;
	}

	private ObjectDslQuery _joinRelationship(
			ObjectRelationship objectRelationship)
		throws PortalException {

		if (_joinStep == null) {
			fromBase();
		}

		long thisObjectDefinitionId =
			_ctx.objectDefinition.getObjectDefinitionId();

		long relatedObjectDefinitionId;

		if (thisObjectDefinitionId ==
				objectRelationship.getObjectDefinitionId1()) {

			relatedObjectDefinitionId =
				objectRelationship.getObjectDefinitionId2();
		}
		else {
			relatedObjectDefinitionId =
				objectRelationship.getObjectDefinitionId1();
		}

		ObjectDefinition relatedObjectDefinition =
			_objectDefinitionLocalService.getObjectDefinition(
				relatedObjectDefinitionId);

		DynamicObjectDefinitionTable relatedBaseTable =
			_createDynamicTable(relatedObjectDefinition);

		DynamicObjectDefinitionTable relatedExtensionTable =
			_createExtensionDynamicTable(relatedObjectDefinition);

		if (Objects.equals(
				objectRelationship.getType(),
				ObjectRelationshipConstants.TYPE_MANY_TO_MANY)) {

			_joinStep = _joinManyToMany(
				objectRelationship, relatedObjectDefinition,
				relatedBaseTable);
		}
		else if (Objects.equals(
					objectRelationship.getType(),
					ObjectRelationshipConstants.TYPE_ONE_TO_MANY)) {

			_joinStep = _joinOneToMany(
				objectRelationship, relatedObjectDefinition,
				relatedBaseTable, relatedExtensionTable);
		}
		else {
			throw new PortalException(
				"Unsupported relationship type " +
					objectRelationship.getType());
		}

		return this;
	}

	private JoinStep _joinManyToMany(
			ObjectRelationship objectRelationship,
			ObjectDefinition relatedObjectDefinition,
			DynamicObjectDefinitionTable relatedBaseTable)
		throws PortalException {

		String pkObjectFieldDBColumnName =
			_ctx.objectDefinition.getPKObjectFieldDBColumnName();
		String relatedPKObjectFieldDBColumnName =
			relatedObjectDefinition.getPKObjectFieldDBColumnName();

		if (objectRelationship.isSelf()) {
			pkObjectFieldDBColumnName += "1";
			relatedPKObjectFieldDBColumnName += "2";
		}

		DynamicObjectRelationshipMappingTable
			dynamicObjectRelationshipMappingTable =
				new DynamicObjectRelationshipMappingTable(
					pkObjectFieldDBColumnName,
					relatedPKObjectFieldDBColumnName,
					objectRelationship.getDBTableName());

		_joinStep = _joinStep.innerJoinON(
			dynamicObjectRelationshipMappingTable,
			dynamicObjectRelationshipMappingTable.getPrimaryKeyColumn2(
			).eq(
				_ctx.primaryKeyColumn
			)
		);

		Predicate predicate;

		if (_ctx.isSystemObject()) {
			predicate =
				dynamicObjectRelationshipMappingTable.getPrimaryKeyColumn1(
				).eq(
					_ctx.systemManager.getPrimaryKeyColumn()
				);
		}
		else {
			predicate =
				dynamicObjectRelationshipMappingTable.getPrimaryKeyColumn1(
				).eq(
					_ctx.baseTable.getPrimaryKeyColumn()
				);
		}

		_joinStep = _joinStep.innerJoinON(
			relatedBaseTable,
			relatedBaseTable.getPrimaryKeyColumn(
			).eq(
				dynamicObjectRelationshipMappingTable.getPrimaryKeyColumn1()
			)
		);

		and(predicate);

		return _joinStep;
	}

	private JoinStep _joinOneToMany(
			ObjectRelationship objectRelationship,
			ObjectDefinition relatedObjectDefinition,
			DynamicObjectDefinitionTable relatedBaseTable,
			DynamicObjectDefinitionTable relatedExtensionTable)
		throws PortalException {

		ObjectField relationshipObjectField =
			_objectFieldLocalService.getObjectField(
				objectRelationship.getObjectFieldId2());

		@SuppressWarnings("unchecked")
		Column<DynamicObjectDefinitionTable, Long>
			relationshipFieldColumn =
				(Column<DynamicObjectDefinitionTable, Long>)
					_objectFieldLocalService.getColumn(
						relatedObjectDefinition.getObjectDefinitionId(),
						relationshipObjectField.getName());

		_joinStep = _joinStep.innerJoinON(
			relatedBaseTable,
			relationshipFieldColumn.eq(_ctx.primaryKeyColumn));

		_joinStep = _joinStep.leftJoinOn(
			relatedExtensionTable,
			relatedBaseTable.getPrimaryKeyColumn(
			).eq(
				relatedExtensionTable.getPrimaryKeyColumn()
			)
		);

		Predicate predicate;

		if (_ctx.isSystemObject()) {
			predicate = relationshipFieldColumn.eq(
				_ctx.systemManager.getPrimaryKeyColumn());
		}
		else {
			predicate = relationshipFieldColumn.eq(
				_ctx.baseTable.getPrimaryKeyColumn());
		}

		and(predicate);

		return _joinStep;
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	private Predicate _eqRelatedField(
			long relatedObjectDefinitionId, String fieldName,
			Serializable value)
		throws PortalException {

		Column column = (Column)_objectFieldLocalService.getColumn(
			relatedObjectDefinitionId, fieldName);

		return column.eq(value);
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
}
