package com.liferay.samples.fbo.object.query.builder;

import com.liferay.object.constants.ObjectRelationshipConstants;
import com.liferay.object.model.ObjectDefinition;
import com.liferay.object.model.ObjectEntryTable;
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
import com.liferay.petra.sql.dsl.query.FromStep;
import com.liferay.petra.sql.dsl.query.JoinStep;
import com.liferay.portal.kernel.exception.PortalException;
import com.liferay.portal.kernel.log.Log;
import com.liferay.portal.kernel.log.LogFactoryUtil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
	
	private boolean _distinct = false;
	
	private final ObjectContextHelper _objectContextHelper;
	private final Map<String, ObjectContext> _contextsByERC = new HashMap<>();
	private final Map<String, Boolean> _objectEntryJoined = new HashMap<>();	

	public ObjectDslQuery(
	    ObjectContext ctx,
	    ObjectContextHelper objectContextHelper,
	    ObjectDefinitionLocalService objectDefinitionLocalService,
	    ObjectFieldLocalService objectFieldLocalService,
	    ObjectRelationshipLocalService objectRelationshipLocalService) {

	    _ctx = ctx;
	    _objectContextHelper = objectContextHelper;
	    _objectDefinitionLocalService = objectDefinitionLocalService;
	    _objectFieldLocalService = objectFieldLocalService;
	    _objectRelationshipLocalService = objectRelationshipLocalService;

	    _contextsByERC.put(_ctx.objectDefinition.getExternalReferenceCode(), _ctx);
	}
	
	public ObjectDslQuery distinct() {
		_distinct = true;
		
		return this;
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
	
	@SuppressWarnings({"rawtypes", "unchecked"})
	public Expression<?> field(String fieldName) throws PortalException {
	    if (_ctx.isSystemObject()) {
	        try {
	            Column sysCol = (Column)_ctx.systemManager.getTable().getColumn(fieldName);
	            if (sysCol != null) {
	                return sysCol;
	            }
	        }
	        catch (Exception ignore) {
	        }
	    }

	    Column col = (Column)_objectFieldLocalService.getColumn(
	        _ctx.objectDefinition.getObjectDefinitionId(), fieldName);

	    if (col == null) {
	        throw new PortalException(
	            "Field '" + fieldName + "' not found on object definition '" +
	                _ctx.objectDefinition.getName() + "'");
	    }

	    return col;
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	public Expression<?> relatedField(String relatedObjectDefinitionERC, String fieldName)
	    throws PortalException {

	    long companyId = _ctx.objectDefinition.getCompanyId();

	    ObjectDefinition relatedObjectDefinition =
	        _objectDefinitionLocalService.getObjectDefinitionByExternalReferenceCode(
	            relatedObjectDefinitionERC, companyId);
	    
	    ObjectContext relatedCtx = _getOrCreateContextByERC(relatedObjectDefinitionERC);

	    Column col = (Column)_objectFieldLocalService.getColumn(
	        relatedCtx.objectDefinition.getObjectDefinitionId(), fieldName);
	    
	    _contextsByERC.put(relatedObjectDefinition.getExternalReferenceCode(), relatedCtx);

	    if (col == null) {
	        throw new PortalException(
	            "Field '" + fieldName + "' not found on related object definition '" +
	                relatedObjectDefinition.getName() + "'");
	    }

	    return col;
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

		FromStep fromStep;
		
		if(_distinct) {
			fromStep = DSLQueryFactoryUtil.selectDistinct(
					selectArray
				);
		} else {
			fromStep = DSLQueryFactoryUtil.select(
					selectArray
				);
		}
		
		if (_ctx.isSystemObject()) {
			_joinStep = fromStep.from(
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
			_joinStep = fromStep.from(
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
	
	public ObjectDslQuery joinRelationship(
	        String objectDefinition1ERC, String objectDefinition1FieldName,
	        String objectDefinition2ERC, String objectDefinition2FieldName)
	    throws PortalException {

	    if (_joinStep == null) {
	        fromBase();
	    }

	    ObjectContext leftCtx = _contextsByERC.get(objectDefinition1ERC);
	    ObjectContext rightCtx = _contextsByERC.get(objectDefinition2ERC);

	    if (leftCtx == null && rightCtx != null) {
	        String tmpERC = objectDefinition1ERC;
	        objectDefinition1ERC = objectDefinition2ERC;
	        objectDefinition2ERC = tmpERC;

	        String tmpField = objectDefinition1FieldName;
	        objectDefinition1FieldName = objectDefinition2FieldName;
	        objectDefinition2FieldName = tmpField;

	        leftCtx = rightCtx;
	        rightCtx = _contextsByERC.get(objectDefinition2ERC);
	    }

	    if (leftCtx == null) {
	        throw new PortalException(
	            "Object '" + objectDefinition1ERC + "' is not present in the query yet. " +
	            "Join it first before using it as join source.");
	    }

	    if (rightCtx == null) {
	    	rightCtx = _getOrCreateContextByERC(objectDefinition2ERC);

	        _contextsByERC.put(objectDefinition2ERC, rightCtx);
	    }
	    
	    if ("externalReferenceCode".equals(objectDefinition1FieldName)) {
	        ObjectContext tmpLeftCtx = leftCtx;

	        if (tmpLeftCtx != null && !tmpLeftCtx.isSystemObject()) {
	            String tmpERC = objectDefinition1ERC;
	            objectDefinition1ERC = objectDefinition2ERC;
	            objectDefinition2ERC = tmpERC;

	            String tmpField = objectDefinition1FieldName;
	            objectDefinition1FieldName = objectDefinition2FieldName;
	            objectDefinition2FieldName = tmpField;

	            ObjectContext tmpCtx = leftCtx;
	            leftCtx = rightCtx;
	            rightCtx = tmpCtx;
	        }
	        
	    }
	    
	    leftCtx = _contextsByERC.get(objectDefinition1ERC);
	    rightCtx = _contextsByERC.get(objectDefinition2ERC);

	    if (rightCtx == null) {
	        rightCtx = _getOrCreateContextByERC(objectDefinition2ERC);
	        _contextsByERC.put(objectDefinition2ERC, rightCtx);
	    }	    

	    if ("externalReferenceCode".equals(objectDefinition2FieldName)) {

	        ObjectContext tmpRightCtx = rightCtx;

	        if (tmpRightCtx == null) {
	            tmpRightCtx = _getOrCreateContextByERC(objectDefinition2ERC);
	        }

	        if (!tmpRightCtx.isSystemObject()) {

	            Column<?, ?> leftCol =
	                _getColumnWithObjectEntrySupport(leftCtx, objectDefinition1FieldName);

	            _joinCustomObjectByExternalReferenceCode(tmpRightCtx, leftCol);

	            _contextsByERC.put(
	                objectDefinition2ERC,
	                tmpRightCtx
	            );

	            return this;
	        }
	    }

	    Column<?, ?> leftCol =
	        _getColumnWithObjectEntrySupport(leftCtx, objectDefinition1FieldName);

	    Column<?, ?> rightCol =
	        _getColumnWithObjectEntrySupport(rightCtx, objectDefinition2FieldName);

	    _joinOnRightColumn(rightCtx, rightCol, leftCol);

	    return this;
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
	
	public ObjectDslQuery or(Predicate predicate) {
		if (_predicate == null) {
			_predicate = predicate;
		}
		else {
			_predicate = _predicate.or(predicate);
		}

		return this;
	}	

	public ObjectDslQuery whereFieldEquals(
			String fieldName, Serializable value)
		throws PortalException {

		Predicate predicate = _eqField(fieldName, value);

		return and(predicate);
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

	@SuppressWarnings({"rawtypes", "unchecked"})
	private Predicate _eqField(String fieldName, Serializable value)
	    throws PortalException {

	    Column column = null;

	    if (_ctx.isSystemObject()) {
	        try {
	            column = _ctx.systemManager.getTable().getColumn(fieldName);
	        }
	        catch (Exception ignore) {
	        }

	        if (column == null) {
	            column = (Column)_objectFieldLocalService.getColumn(
	                _ctx.objectDefinition.getObjectDefinitionId(), fieldName);
	        }
	    }
	    else {
	        column = (Column)_objectFieldLocalService.getColumn(
	            _ctx.objectDefinition.getObjectDefinitionId(), fieldName);
	    }

	    if (column == null) {
	        throw new PortalException(
	            "Field '" + fieldName + "' not found on object definition '" +
	                _ctx.objectDefinition.getName() + "'");
	    }

	    return column.eq(value);
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
	
	@SuppressWarnings({"rawtypes", "unchecked"})
	private Column<?, ?> _getColumnWithObjectEntrySupport(
	    ObjectContext ctx, String fieldName)
	throws PortalException {

	    if (ctx.isSystemObject()) {
	        Column col = ctx.systemManager.getTable().getColumn(fieldName);
	        if (col != null) {
	            return col;
	        }
	    }

	    if ("externalReferenceCode".equals(fieldName)) {
	        String erc = ctx.objectDefinition.getExternalReferenceCode();

	        if (!_objectEntryJoined.containsKey(erc)) {
	            throw new PortalException(
	                "Cannot use externalReferenceCode for '" + erc +
	                "' because ObjectEntry is not joined yet. " +
	                "Join the custom object via externalReferenceCode first.");
	        }

	        return ObjectEntryTable.INSTANCE.externalReferenceCode;
	    }

	    Column col = (Column)_objectFieldLocalService.getColumn(
	        ctx.objectDefinition.getObjectDefinitionId(), fieldName);

	    if (col == null) {
	        throw new PortalException(
	            "Field '" + fieldName + "' not found on object definition '" +
	            ctx.objectDefinition.getName() + "'");
	    }

	    return col;
	}	

	private ObjectContext _getOrCreateContextByERC(String erc)
	    throws PortalException {

	    ObjectContext cached = _contextsByERC.get(erc);
	    if (cached != null) {
	        return cached;
	    }

	    long companyId = _ctx.objectDefinition.getCompanyId();

	    ObjectDefinition od =
	        _objectDefinitionLocalService.getObjectDefinitionByExternalReferenceCode(
	            erc, companyId);

	    ObjectContext ctx = _objectContextHelper.createContext(od);

	    _contextsByERC.put(erc, ctx);

	    return ctx;
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	private void _joinCustomObjectByExternalReferenceCode(
	        ObjectContext rightCtx, Column<?, ?> leftErcColumn) {

	    String rightErc = rightCtx.objectDefinition.getExternalReferenceCode();

	    if (_objectEntryJoined.containsKey(rightErc)) {
	        return;
	    }

	    _joinStep = _joinStep.innerJoinON(
	        ObjectEntryTable.INSTANCE,
	        ObjectEntryTable.INSTANCE.externalReferenceCode.eq((Column)leftErcColumn)
	    );

	    _joinStep = _joinStep.innerJoinON(
	        rightCtx.baseTable,
	        rightCtx.baseTable.getPrimaryKeyColumn().eq(
	            ObjectEntryTable.INSTANCE.objectEntryId
	        )
	    );

	    _joinStep = _joinStep.leftJoinOn(
	        rightCtx.extensionTable,
	        rightCtx.baseTable.getPrimaryKeyColumn().eq(
	            rightCtx.extensionTable.getPrimaryKeyColumn()
	        )
	    );

	    _objectEntryJoined.put(rightErc, true);
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
	    Column<DynamicObjectDefinitionTable, Long> fkColumn =
	        (Column<DynamicObjectDefinitionTable, Long>)
	            _objectFieldLocalService.getColumn(
	                relatedObjectDefinition.getObjectDefinitionId(),
	                relationshipObjectField.getName());

	    Column<?, Long> onePK = _ctx.primaryKeyColumn;

	    String fkTableName = fkColumn.getTable().getName();
	    boolean fkOnExtension =
	        fkTableName.equals(relatedExtensionTable.getName());

	    if (fkOnExtension) {
	        _joinStep = _joinStep.innerJoinON(
	            relatedExtensionTable,
	            fkColumn.eq(onePK)
	        );

	        _joinStep = _joinStep.innerJoinON(
	            relatedBaseTable,
	            relatedBaseTable.getPrimaryKeyColumn().eq(
	                relatedExtensionTable.getPrimaryKeyColumn()
	            )
	        );
	    }
	    else {
	        _joinStep = _joinStep.innerJoinON(
	            relatedBaseTable,
	            fkColumn.eq(onePK)
	        );

	        _joinStep = _joinStep.leftJoinOn(
	            relatedExtensionTable,
	            relatedBaseTable.getPrimaryKeyColumn().eq(
	                relatedExtensionTable.getPrimaryKeyColumn()
	            )
	        );
	    }

	    return _joinStep;
	}
	
	@SuppressWarnings({"rawtypes", "unchecked"})
	private void _joinOnRightColumn(
	    ObjectContext rightCtx, Column<?, ?> rightJoinColumn, Column<?, ?> leftJoinColumn) {

	    String rightJoinTableName = rightJoinColumn.getTable().getName();
	    String rightExtTableName = rightCtx.extensionTable.getName();

	    boolean joinOnExtension = rightExtTableName.equals(rightJoinTableName);

	    if (joinOnExtension) {
	        _joinStep = _joinStep.innerJoinON(
	            rightCtx.extensionTable,
	            ((Column)rightJoinColumn).eq(leftJoinColumn)
	        );

	        if (rightCtx.isSystemObject()) {
	            _joinStep = _joinStep.innerJoinON(
	                rightCtx.getRootTable(),
	                rightCtx.systemManager.getPrimaryKeyColumn().eq(
	                    rightCtx.extensionTable.getPrimaryKeyColumn())
	            );
	        }
	        else {
	            _joinStep = _joinStep.innerJoinON(
	                rightCtx.baseTable,
	                rightCtx.baseTable.getPrimaryKeyColumn().eq(
	                    rightCtx.extensionTable.getPrimaryKeyColumn())
	            );
	        }

	        return;
	    }

	    _joinStep = _joinStep.innerJoinON(
	        rightCtx.getRootTable(),
	        ((Column)rightJoinColumn).eq(leftJoinColumn)
	    );

	    if (rightCtx.isSystemObject()) {
	        _joinStep = _joinStep.leftJoinOn(
	            rightCtx.extensionTable,
	            rightCtx.systemManager.getPrimaryKeyColumn().eq(
	                rightCtx.extensionTable.getPrimaryKeyColumn())
	        );
	    }
	    else {
	        _joinStep = _joinStep.leftJoinOn(
	            rightCtx.extensionTable,
	            rightCtx.baseTable.getPrimaryKeyColumn().eq(
	                rightCtx.extensionTable.getPrimaryKeyColumn())
	        );
	    }
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
	
	private static final Log _log = LogFactoryUtil.getLog(
			ObjectDslQuery.class);
	
}
