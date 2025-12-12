package com.liferay.samples.fbo.object.query.builder.test;

import com.liferay.object.service.ObjectDefinitionLocalService;
import com.liferay.object.service.persistence.ObjectEntryPersistence;
import com.liferay.petra.sql.dsl.query.DSLQuery;
import com.liferay.portal.kernel.exception.PortalException;
import com.liferay.samples.fbo.object.query.builder.ObjectDslQueryBuilder;

import java.util.List;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;

@Component(
	property = {
		"osgi.command.function=testQuery",
		"osgi.command.scope=osram"
	},
	service = TestQueryCommand.class
)
public class TestQueryCommand extends AbstractTestQueryCommand {

	private static final String C_PRODUCT_OBJECT_DEFINITION_ERC =
		"L_COMMERCE_PRODUCT_DEFINITION";

	private static final String
		C_PRODUCT_ADDITIONAL_DATA_OBJECT_DEFINITION_ERC =
			"productAdditionalData";

	@Override
	protected void _doTestQuery(long companyId) throws PortalException {

		DSLQuery query = _objectDslQueryBuilder.forCompany(companyId)
			.forDefinition(
					C_PRODUCT_OBJECT_DEFINITION_ERC
			).distinct(
			).selectPrimaryKey(
			).fromBase(
			).joinRelationship(
					"productNo"
			).whereFieldEquals(
					"shippable", true
			).whereRelatedFieldEquals(
					C_PRODUCT_ADDITIONAL_DATA_OBJECT_DEFINITION_ERC,
					"orderReference", "64193DR1"
			).build();

		System.out.println("DSLQuery = " + query);

		List<Long> response = _objectEntryPersistence.dslQuery(query);

		response.forEach(
			item -> System.out.println("Row: " + item)
		);
	}

	@Reference
	private ObjectDslQueryBuilder _objectDslQueryBuilder;

	@Reference
	private ObjectEntryPersistence _objectEntryPersistence;

	@Reference
	private ObjectDefinitionLocalService _objectDefinitionLocalService;

}
