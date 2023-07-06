from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Literal


class Granularity(Enum, str):
    """Granularity options."""

    GRANULARITY_UNSPECIFIED = "GRANULARITY_UNSPECIFIED"
    SUB_PREMISE = "SUB_PREMISE"
    PREMISE = "PREMISE"
    PREMISE_PROXIMITY = "PREMISE_PROMXIMITY"
    BLOCK = "BLOCK"
    ROUTE = "ROUTE"
    OTHER = "OTHER"


@dataclass
class GoogleAddressValidationVerdict:
    """Validation results and metadata."""

    inputGranularity: Granularity = "GRANULARITY_UNSPECIFIED"
    validationGranularity: Granularity = "GRANULARITY_UNSPECIFIED"
    geocodeGranularity: Granularity = "GRANULARITY_UNSPECIFIED"
    hasInferredComponents: bool = False
    hasUnconfirmedComponents: bool = False
    hasReplacedComponents: bool = False
    addressComplete: bool = False


@dataclass
class GoogleAddressValidationPostalAddress:
    regionCode: str
    languageCode: str
    postalCode: str
    administrativeArea: str
    locality: str
    addressLines: List[str]
    revision: int = 0
    sortingCode: str = ""
    sublocality: str = ""
    recipients: List[str] = field(default_factory=list)
    organization: str = ""


@dataclass
class GoogleAddressValidationAddressComponent:
    componentName: Dict[Literal["text", "languageCode"], str]
    componentType: str
    confirmationLevel: Literal[
        "CONFIRMED",
        "UNCONFIRMED_AND_SUSPICIOUS",
        "UNCONFIRMED_BUT_PLAUSIBLE",
        "CONFIRMATION_LEVEL_UNSPECIFIED",
    ] = "CONFIRMATION_LEVEL_UNSPECIFIED"
    inferred: bool = False
    spellCorrected: bool = False
    replaced: bool = False
    unexpected: bool = False


@dataclass
class GoogleAddressValidationAddress:
    formattedAddress: str
    postalAddress: GoogleAddressValidationPostalAddress
    addressComponents: List[GoogleAddressValidationAddressComponent]
    missingComponentTypes: List[str] = field(default_factory=list)
    unconfirmedComponentTypes: List[str] = field(default_factory=list)
    unresolvedTokens: List[str] = field(default_factory=list)


@dataclass
class LatLon:
    latitude: float
    longitude: float


@dataclass
class LatLonBounds:
    low: LatLon
    high: LatLon


@dataclass
class GoogleAddressValidationGeocode:
    location: LatLon
    plusCode: Dict[Literal["globalCode", "compoundCode"], str]
    bounds: LatLonBounds
    featureSizeMeters: float
    placeId: str
    placeTypes: List[str]


@dataclass
class GoogleAddressValidationMetadata:
    business: bool = False
    poBox: bool = False
    residential: bool = False


@dataclass
class GoogleAddressValidationUspsData:
    standardizedAddress: Dict[
        Literal[
            "firstAddressLine",
            "firm",
            "secondAddressLine",
            "urbanization",
            "cityStateZipAddressLine",
            "city",
            "state",
            "zipCode",
            "zipCodeExtension",
        ],
        str,
    ]
    deliveryPointCode: str
    deliveryPointCheckDigit: str
    dpvConfirmation: str
    dpvFootnote: str
    dpvCmra: str
    dpvVacant: str
    dpvNoStat: str
    carrierRoute: str
    carrierRouteIndicator: str
    postOfficeCity: str
    postOfficeState: str
    fipsCountyCode: str
    county: str
    elotNumber: str
    elotFlag: str
    addressRecordType: str
    defaultAddress: bool
    ewsNoMatch: bool = False
    abbreviatedCity: str = ""
    lacsLinkReturnCode: str = ""
    lacsLinkIndicator: str = ""
    poBoxOnlyPostalCode: bool = False
    suitelinkFootnote: str = ""
    pmbDesignator: str = ""
    pmbNumber: str = ""
    errorMessage: str = ""
    cassProcessed: bool = False


@dataclass
class GoogleAddressValidationResult:
    verdict: GoogleAddressValidationVerdict
    address: GoogleAddressValidationAddress
    geocode: GoogleAddressValidationGeocode
    metadata: GoogleAddressValidationMetadata
    uspsData: GoogleAddressValidationUspsData
