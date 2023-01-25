"""Tasks related to attribute configurations as part of Tamr projects"""
from tamr_unify_client.project.attribute_configuration.resource import AttributeConfigurationSpec

from tamr_unify_client.project.resource import Project

import logging

LOGGER = logging.getLogger(__name__)


def _check_tokenizer(value):
    """Check tokenizer value is valid

    Args:
        value: Value of variable to check

    Raises:
        ValueError: Raised if value if not in allowed list
    """
    # List of allowed values
    allowed_values = ["DEFAULT", "STEMMING_EN", "BIGRAM", "TRIGRAM", "BI-WORD", "REGEX"]

    # Raise error if not in allowed list
    if value not in allowed_values:
        raise ValueError(
            f"Value '{value}' not valid for attribute configuration variable "
            + "tokenizer. Allowed values: {allowed_values}"
        )


def _check_similarity_function(value):
    """Check similarityFunction value is valid

    Args:
        value: Value of variable to check

    Raises:
        ValueError: Raised if value if not in allowed list
    """
    # List of allowed values
    allowed_values = ["COSINE", "JACCARD", "ABSOLUTE_DIFF", "RELATIVE_DIFF"]

    # Raise error if not in allowed list
    if value not in allowed_values:
        raise ValueError(
            f"Value '{value}' not valid for attribute configuration variable "
            + "similarityFunction. Allowed values: {allowed_values}"
        )


def _check_attribute_role(value):
    """Check attributeRole value is valid

    Args:
        value: Value of variable to check

    Raises:
        ValueError: Raised if value if not in allowed list
    """
    # List of allowed values
    allowed_values = ["CLUSTER_NAME_ATTRIBUTE", "SUM_ATTRIBUTE", ""]

    # Raise error if not in allowed list
    if value not in allowed_values:
        raise ValueError(
            f"Value '{value}' not valid for attribute configuration variable "
            + "attributeRole. Allowed values: {allowed_values}"
        )


def _check_enabled_for_ml(value):
    """Check enableForMl value is valid

    Args:
        value: Value of variable to check

    Raises:
        ValueError: Raised if value if not in allowed list
    """
    # List of allowed values
    allowed_values = [True, False]

    # Raise error if not in allowed list
    if value not in allowed_values:
        raise ValueError(
            f"Value '{value}' not valid for attribute configuration variable "
            + "enabledForMl. Allowed values: {allowed_values}"
        )


def get_attribute_configurations(project: Project) -> AttributeConfigurationSpec:
    """Store the attribute configurations in a list for a given project

    Args:
        project: Project containing attributes with configurations defined

    Returns:
        List of AttributeConfigurationSpec
    """

    # Get all the attribute configurations from a project
    attribute_configuration_all = project.attribute_configurations()

    # Itterate through all attributes and store in a list
    attribute_configuration_spec = [
        attribute_configuration.spec() for attribute_configuration in attribute_configuration_all
    ]

    return attribute_configuration_spec


def update_attribute_configuration(
    project: Project,
    attributeName: str,
    attributeRole: str = None,
    similarityFunction: str = None,
    enabledForMl: bool = None,
    tokenizer: str = None,
    numericFieldResolution: float = None,
):
    """Update the attribute configuration variables of a given
    attribute in a project

    Args:
        project (Project): Project in which attribute is present
        attributeName (str): Name of attribute to update configuration
        attributeRole (str, optional): The specific role, if any, of the attribute in the project.
            Defaults to None.
        similarityFunction (str, optional): The similarity function to use for the unified dataset
            attribute. Defaults to None.
        enabledForMl (bool, optional): Whether the unified dataset attribute is being included in
            machine learning operations. Defaults to None.
        tokenizer (str, optional): The tokenizer used for tokenizing text values. Defaults to
            None.
        numericFieldResolution (float, optional): Indicates how to process numeric values.
            Defaults to None.

    Raises:
        RuntimeError: If the attribute is not found in the project
        ValueError: If any of the optional attribute configuration values are not valid

    Returns:
        Response of API request to update attribute configuration
    """
    LOGGER = logging.getLogger(__name__)

    # Define variable for use later
    attribute_found = False
    attribute_variable_dict = {}

    # Check project has attribute present
    project_attribute_configuration_all = get_attribute_configurations(project=project)
    # Get the values from the configurations

    # Loop through all the attributes
    for attribute_config_single in project_attribute_configuration_all:
        # Get values from configuration spec
        attribute_config_single_dict = attribute_config_single.to_dict()
        # Check if name matches
        if attribute_config_single_dict["attributeName"] == attributeName:
            # Set to true if match
            attribute_found = True
            # Save attribute info
            attribute_config_spec = attribute_config_single
            attribute_config = attribute_config_single_dict
    if attribute_found:
        LOGGER.info(f"Attribute {attributeName} sucessfully found in project {project.name}")
        pass
    else:
        raise RuntimeError(f"Attribute {attributeName} not in project {project.name}!")

    # Check there are fields to be updated
    variable_list = [
        attributeRole,
        similarityFunction,
        enabledForMl,
        tokenizer,
        numericFieldResolution,
    ]
    empty_variables = all(variable is None for variable in variable_list)
    if empty_variables:
        raise ValueError(
            "No configuration variables are specified. Enter a value for at least one"
            + "of the following variables: <attributeRole>, <similarityFunction>,"
            + "<enabledForMl>, <tokenizer>, <numericFieldResolution>!"
        )

    # Collect configuration variables to update
    if attributeRole is not None:
        _check_attribute_role(attributeRole)
        attribute_variable_dict["attributeRole"] = attributeRole
    if similarityFunction is not None:
        _check_similarity_function(similarityFunction)
        attribute_variable_dict["similarityFunction"] = similarityFunction
    if enabledForMl is not None:
        _check_enabled_for_ml(enabledForMl)
        attribute_variable_dict["enabledForMl"] = enabledForMl
    if tokenizer is not None:
        _check_tokenizer(tokenizer)
        attribute_variable_dict["tokenizer"] = tokenizer
    if numericFieldResolution is not None:
        attribute_variable_dict["numericFieldResolution"] = numericFieldResolution

    # Loop through new config values and check for changes
    for key, value in attribute_variable_dict.items():
        # Check if an update
        if value == attribute_config[key]:
            LOGGER.warn(f"Attribute configuration variable {key} will not change!")
            pass
        else:
            LOGGER.info(
                f"Attribute configuration variable {key} will be updated "
                + f"from {attribute_config[key]} to {value}!"
            )
            # Update attribute configuration value which has changed
            attribute_config[key] = value

    # Send updates to Tamr
    new_attribute_configuration = attribute_config_spec.from_data(attribute_config)
    response = new_attribute_configuration.put()

    return response
