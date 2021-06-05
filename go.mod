module github.com/open-cluster-management/leaf-hub-spec-sync

go 1.16

require (
	github.com/open-cluster-management/hub-of-hubs-data-types v0.0.0-20210604065451-a13560dc2c3e
	github.com/open-horizon/edge-sync-service-client v0.0.0-20190711093406-dc3a19905da2
	github.com/open-horizon/edge-utilities v0.0.0-20190711093331-0908b45a7152 // indirect
	k8s.io/apimachinery v0.21.1
	k8s.io/client-go v12.0.0+incompatible
)

replace k8s.io/client-go => k8s.io/client-go v0.21.1
