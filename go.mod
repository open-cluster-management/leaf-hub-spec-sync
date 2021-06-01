module github.com/open-cluster-management/leaf-hub-spec-sync

go 1.16

require (
	cloud.google.com/go v0.54.0 // indirect
	github.com/Azure/go-autorest/autorest/adal v0.9.5 // indirect
	github.com/open-cluster-management/governance-policy-propagator v0.0.0-20210520203318-a78632de1e26
	github.com/open-horizon/edge-sync-service-client v0.0.0-20190711093406-dc3a19905da2
	github.com/open-horizon/edge-utilities v0.0.0-20190711093331-0908b45a7152 // indirect
	k8s.io/api v0.21.1 // indirect
	k8s.io/apimachinery v0.21.1 // indirect
	k8s.io/client-go v12.0.0+incompatible
	k8s.io/utils v0.0.0-20201110183641-67b214c5f920 // indirect
)

replace k8s.io/client-go => k8s.io/client-go v0.21.1
