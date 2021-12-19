module github.com/open-cluster-management/leaf-hub-spec-sync

go 1.16

require (
	github.com/confluentinc/confluent-kafka-go v1.7.0
	github.com/go-logr/logr v0.2.1
	github.com/go-logr/zapr v0.2.0 // indirect
	github.com/open-cluster-management/hub-of-hubs-data-types v0.2.3
	github.com/open-cluster-management/hub-of-hubs-kafka-transport v0.0.0-20211123103330-4a0b4f760f84
	github.com/open-cluster-management/hub-of-hubs-message-compression v0.2.0
	github.com/open-horizon/edge-sync-service-client v0.0.0-20190711093406-dc3a19905da2
	github.com/open-horizon/edge-utilities v0.0.0-20190711093331-0908b45a7152 // indirect
	github.com/operator-framework/operator-sdk v0.19.4
	github.com/spf13/pflag v1.0.5
	k8s.io/apimachinery v0.20.5
	k8s.io/client-go v12.0.0+incompatible
	sigs.k8s.io/controller-runtime v0.6.2
)

replace k8s.io/client-go => k8s.io/client-go v0.20.5
