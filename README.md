[comment]: # ( Copyright Contributors to the Open Cluster Management project )

# Leaf-Hub Spec Sync

[![Go Report Card](https://goreportcard.com/badge/github.com/open-cluster-management/leaf-hub-spec-sync)](https://goreportcard.com/report/github.com/open-cluster-management/leaf-hub-spec-sync)
[![Go Reference](https://pkg.go.dev/badge/github.com/open-cluster-management/leaf-hub-spec-sync.svg)](https://pkg.go.dev/github.com/open-cluster-management/leaf-hub-spec-sync)
[![License](https://img.shields.io/github/license/open-cluster-management/leaf-hub-spec-sync)](/LICENSE)

The leaf hub spec sync component of [Hub-of-Hubs](https://github.com/open-cluster-management/hub-of-hubs).

Go to the [Contributing guide](CONTRIBUTING.md) to learn how to get involved.

## Getting Started

## Build and push the image to docker registry

1.  Set the `REGISTRY` environment variable to hold the name of your docker registry:
    ```
    $ export REGISTRY=...
    ```
    
1.  Set the `IMAGE_TAG` environment variable to hold the required version of the image.  
    default value is `latest`, so in that case no need to specify this variable:
    ```
    $ export IMAGE_TAG=latest
    ```
    
1.  Run make to build and push the image:
    ```
    $ make push-images
    ```

## Deploy on a leaf hub

1.  Set the `REGISTRY` environment variable to hold the name of your docker registry:
    ```
    $ export REGISTRY=...
    ```

1.  Set the `IMAGE` environment variable to hold the name of the image.
    ```
    $ export IMAGE=$REGISTRY/$(basename $(pwd)):latest
    ```
    
1. Set the `LH_ID` environment variable to hold the leaf hub unique id.
   ```
   $ export LH_ID=...
   ``` 

1. Set the `TRANSPORT_TYPE` environment variable to "kafka" or "sync-service" to set which transport to use.
    ```
    $ export TRANSPORT_TYPE=...
    ```

1. If you set kafka as transport, set the following environment variables:
    1. Set `KAFKA_BOOTSTRAP_SERVERS` environment variable to hold the
       address of the brokers to connect to.
        ```
        $ export KAFKA_BOOTSTRAP_SERVERS=...
        ```
    
    1. If you use secure (SSL/TLS) client authorization, set `KAFKA_SSL_CA` environment variable to hold the
       certificate (PEM format) encoded in base64.
        ```
        $ export KAFKA_SSL_CA=$(cat PATH_TO_CA | base64 -w 0)
        ```

1. Otherwise, if you set sync-service as transport, set the following:
    1. Set the `SYNC_SERVICE_PORT` environment variable to hold the ESS port as was setup in the leaf hub.
        ```
        $ export SYNC_SERVICE_PORT=...
        ```
   
1.  Set the `K8S_CLIENTS_POOL_SIZE` environment variable to hold the size of the k8s clients pool.
    The pool is used to run update and delete operations of the received bundle's objects concurrently.
    The environment variable is optional. Default value is 10. 
    ```
    $ export K8S_CLIENTS_POOL_SIZE=...
    ```
    
1.  Run the following command to deploy the `leaf-hub-spec-sync` to your leaf hub cluster:  
    ```
    envsubst < deploy/leaf-hub-spec-sync.yaml.template | kubectl apply -f -
    ```
    
## Cleanup from a leaf hub
    
1.  Run the following command to clean `leaf-hub-spec-sync` from your leaf hub cluster:  
    ```
    envsubst < deploy/leaf-hub-spec-sync.yaml.template | kubectl delete -f -
    ```

