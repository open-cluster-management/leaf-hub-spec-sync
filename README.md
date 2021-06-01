[comment]: # ( Copyright Contributors to the Open Cluster Management project )

# Leaf-Hub Spec Sync
Red Hat Advanced Cluster Management Leaf Hub Spec Sync  

## Deploy on a leaf hub

1.  Set the `SYNC_SERVICE_PORT` environment variable to hold the ESS host as was setup in the leaf hub.
    ```
    $ export SYNC_SERVICE_PORT=...
    ```
    
1.  Run the following command to deploy the ESS to your leaf hub cluster:  
    ```
    envsubst < deploy/leaf-hub-spec-sync.yaml.template | kubectl apply -f -
    ```
    
edge-sync-service ESS k8s objects will be created under the namespace `sync-service`.
    
#### Cleanup of ESS from a leaf hub
    
1.  Run the following command to clean leaf-hub-spec-sync from your leaf hub cluster:  
    ```
    envsubst < deploy/leaf-hub-spec-sync.yaml.template | kubectl delete -f -
    ```

