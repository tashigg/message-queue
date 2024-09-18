FoxMQ Demo Cluster
==================

Deployed to project `foxmq-432407` in Google Cloud.

As deployed, this only supports MQTT connections over Websockets/HTTPS (`wss://`).

Credentials for the currently deployed instance are available on Notion:
<https://www.notion.so/tashi-network/FoxMQ-Demo-Credentials-a1ab231d17ca4404acd34ec30fc01d20?pvs=4>

Architecture
----

Uses a Kubernetes [`StatefulSet`] for the sticky pod identities so separate service IPs
can be assigned to each node.

It was intended that each node has its own MQTT user credentials and should be assigned its own domain name
so that the cluster may be demonstrated to be highly available.

TLS is provided via [Google-managed SSL certificates][gke-ssl].

### Note: Region-Specific Config

`ingress.yaml` and `managed-certificate.yaml` have region-specific configs:

* `ingress.yaml` requires the global static IP name which must be specific to each region.
* `ingress.yaml` and `managed-certificate.yaml` require listing out the region-specific hostnames 
  used for routing and SSL certificate generation, respectively.

These can be found in subdirectories named by GCloud region.

### Note: Changing Replica Count

As of writing, the configured replica count of the cluster is `4`.

Due to API limitations, several parts of the configuration are hardcoded with this number, and they **MUST** match
for proper operation:

* `stateful-set.yaml`: the `StatefulSet` configuration itself (and the ancillary headless service)
  * `.spec.replicas`
    * Controls the number of pods created by the `StatefulSet`
  * `.spec.template.spec.initContainers[name='create-foxmq-d'].env[name='NUM_REPLICAS']`
    * Controls the number of entries generated in each node's address book.
* `service-cluster.yaml`: services that allow nodes to talk to each other
  * Configures a different `Service` object for each replica. 
  * Entries will need to be added or removed to match the replica count.
  * Use the label `statefulset.kubernetes.io/pod-name: foxmq-demo-N` to select node `N` (starting from zero).
* `service-websockets.yaml`: services that allow nodes to accept Websocket connections
  * Configures a different `Service` object for each replica.
  * Entries will need to be added or removed to match the new replica count.
  * Use the label `statefulset.kubernetes.io/pod-name: foxmq-demo-N` to select node `N` (starting from zero).
* `ingress.yaml`: the `Ingress` object which accepts and routes HTTP(S) connections (including websockets)
  * Entries in `.spec.hosts` will need to be added or removed to match the new replica count.
  * Each entry matches a Service, by name, that was defined in `service-websockets.yaml`.
  * A Cloud DNS entry in Gcloud already exists with a wildcard domain for `*.demo.foxmq.infra.tashi.dev`;
    no DNS changes should be necessary if updating the existing Ingress.
* `managed-certificate.yaml`: configures the SSL certificate used by the GKE Ingress controller.
  * Entries in `.spec.domains` will need to be added or removed to match the new replica count.

[`StatefulSet`]: https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/
[gke-ssl]: https://cloud.google.com/kubernetes-engine/docs/how-to/managed-certs

Installation
----

### Generate Secrets

A shell script is provided to create the required secrets for the operation of the cluster:

* P-256 keypairs (public and private keys)
  * Secret `foxmq-demo-secret-keys` and ConfigMap `foxmq-demo-public-keys`
  * Requires the `openssl` CLI (`sudo apt install openssl`)
* User credentials for each node
    * Secret `foxmq-demo-user-credentials` and ConfigMap `foxmq-demo-users` 
    * Runs the `foxmq` binary via `cargo run`. 
    * Working directory should be `k8s/demo` (the directory containing this README).

The shell script accepts two arguments: the number of nodes in the cluster as an integer, and `--update`.

Any trailing arguments are passed to all `kubectl` commands invoked by the script (e.g. `--context` or `--namespace`).

#### Create New Secrets

This command will create keypairs and user credentials for 4 nodes (the current specified in `stateful-set.yaml`)

```shell
$ ./gen-secrets.sh 4
```

#### Overwrite Existing

Replaces the existing keypairs and user credentials for the given number of nodes.

```shell
$ ./gen-secrets.sh 4 --update
```

#### Get User Credentials

Each node `N` (starting from zero) gets a pair of user credentials generated: username `demo-N` and a random password.

To get the password for a given user (requires [`jq`](https://jqlang.github.io/jq/)):

##### Print Password to Shell (not recommended)

E.g., for user `demo-0` on `0.demo.foxmq.infra.tashi.dev`:
```shell
$ kubectl get secret foxmq-demo-user-credentials -o json | jq -r '.data."demo-0"' | base64 --decode
```

##### Copy Password to Clipboard 

Requires [`xclip`](https://launchpad.net/ubuntu/+source/xclip) (Linux only)

E.g., for user `demo-0` on `0.demo.foxmq.infra.tashi.dev`:
```shell
$ kubectl get secret foxmq-demo-user-credentials -o json | jq -r '.data."demo-0"' | base64 --decode | xclip -selection c
```

### Allocate a Static IP

Requires the Gcloud CLI

#### Create the Address
```shell
$ gloud compute addresses create --global foxmq-demo
```

#### Get the Allocated IP

```shell
$ gcloud compute addresses list --global
```

Take the IP address and create the appropriate DNS entry.

### Install or Update Cluster

Simply `kubectl apply` the relevant files (unchanged files may be omitted if updating):
```shell
$ kubectl apply -f stateful-set.yaml
$ kubectl apply -f service-cluster.yaml
$ kubectl apply -f backend-config.yaml
$ kubectl apply -f service-websockets.yaml
$ kubectl apply -f managed-certificate.yaml
$ kubectl apply -f ingress.yaml
```

### Block Insecure Connections

Annotate the Ingress to block connections over plaintext HTTP. 

The allocation of the load balancer will fail if this annotation is on the Ingress to begin with, so it needs
to be added after the fact.

```shell
$ kubectl annotate ingress foxmq-demo kubernetes.io/ingress.allow-http='false'
```
