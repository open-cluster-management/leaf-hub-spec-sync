package bundle

type ObjectsBundle struct {
	Objects        []interface{} `json:"objects"`
	DeletedObjects []interface{} `json:"deletedObjects"`
}
