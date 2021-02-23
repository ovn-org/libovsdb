package libovsdb

var (
	aString  = "foo"
	aEnum    = "enum1"
	aEnumSet = []string{"enum1", "enum2", "enum3"}
	aSet     = []string{"a", "set", "of", "strings"}
	aUUID0   = "2f77b348-9768-4866-b761-89d5177ecda0"
	aUUID1   = "2f77b348-9768-4866-b761-89d5177ecda1"
	aUUID2   = "2f77b348-9768-4866-b761-89d5177ecda2"
	aUUID3   = "2f77b348-9768-4866-b761-89d5177ecda3"

	aUUIDSet = []string{
		aUUID0,
		aUUID1,
		aUUID2,
		aUUID3,
	}

	aIntSet = []int{
		0,
		1,
		2,
		3,
	}
	aFloat = 42.00

	aFloatSet = []float64{
		3.14,
		2.71,
		42.0,
	}

	aMap = map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	aEmptySet = []string{}
)
