[comment]: # ( Copyright Contributors to the Open Cluster Management project )

# Leaf-Hub Spec Sync
Red Hat Advanced Cluster Management Leaf Hub Spec Sync  

## Build the image

1.  Set the `REGISTRY` environment variable to hold the name of your docker registry:
    ```
    $ export REGISTRY=...
    ```
    
1.  Set the `IMAGE` environment variable to hold the name of the image.

    ```
    $ export IMAGE=$REGISTRY/$(basename $(pwd)):latest
    ```
    
1.  Run make to build the image and push it to docker registry:
    ```
    make docker-push
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

1.  Set the `SYNC_SERVICE_PORT` environment variable to hold the ESS port as was setup in the leaf hub.
    ```
    $ export SYNC_SERVICE_PORT=...
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

