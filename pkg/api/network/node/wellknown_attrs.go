package neofsnode

// Enumeration of well-known attributes.
const (
	// AttrPrice is a key to the node attribute that indicates the
	// price in GAS tokens for storing one GB of data during one Epoch.
	AttrPrice = "Price"

	// AttrCapacity is a key to the node attribute that indicates the
	// total available disk space in Gigabytes.
	AttrCapacity = "Capacity"

	// AttrSubnet is a key to the node attribute that indicates the
	// string ID of node's storage subnet.
	AttrSubnet = "Subnet"

	// AttrUNLOCODE is a key to the node attribute that indicates the
	// node's geographic location in UN/LOCODE format.
	AttrUNLOCODE = "UN-LOCODE"

	// AttrCountryCode is a key to the node attribute that indicates the
	// Country code in ISO 3166-1_alpha-2 format.
	AttrCountryCode = "CountryCode"

	// AttrCountry is a key to the node attribute that indicates the
	// country short name in English, as defined in ISO-3166.
	AttrCountry = "Country"

	// AttrLocation is a key to the node attribute that indicates the
	// place name of the node location.
	AttrLocation = "Location"

	// AttrSubDivCode is a key to the node attribute that indicates the
	// country's administrative subdivision where node is located
	// in ISO 3166-2 format.
	AttrSubDivCode = "SubDivCode"

	// AttrSubDiv is a key to the node attribute that indicates the
	// country's administrative subdivision name, as defined in
	// ISO 3166-2.
	AttrSubDiv = "SubDiv"

	// AttrContinent is a key to the node attribute that indicates the
	// node's continent name according to the Seven-Continent model.
	AttrContinent = "Continent"
)
