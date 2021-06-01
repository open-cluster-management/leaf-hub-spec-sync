[comment]: # ( Copyright Contributors to the Open Cluster Management project )

# Leaf-Hub Spec Sync
Red Hat Advanced Cluster Management Leaf Hub Spec Sync  

## How it works

## Build to run locally

```
make
```

## Run Locally

Set the following environment variables:

* HOH_TRANSPORT_SYNC_INTERVAL - the interval between subsequent periodic syncs from Hub-of-Hubs to leaf hubs.  
    The expected format is `number(units), e.g. 10s
* SYNC_SERVICE_PROTOCOL
* SYNC_SERVICE_HOST
* SYNC_SERVICE_PORT

```
./build/bin/hoh-transport-bridge
```
