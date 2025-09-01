# Getting started

This walks through running the plugin against a real PipeCD control plane, a real piped, and a real
Kafka cluster (Redpanda) — all on your own machine, no managed services or accounts required. It's the
exact path used to develop and verify this plugin, gotchas included.

If you just want to read what the plugin does without running anything, see the [README](../README.md)
instead — the "How it works" and "Why this is not just apply the YAML" sections cover the design.

## What you need

| Tool | Used for | Tested with |
|---|---|---|
| [Go](https://go.dev) | building the plugin, and piped itself | 1.24+ |
| [Docker](https://www.docker.com/products/docker-desktop/) | Kafka (Redpanda) and the kind cluster | 24+ |
| [kind](https://kind.sigs.k8s.io) | a local Kubernetes cluster for the control plane | 0.20+ |
| [kubectl](https://kubernetes.io/docs/tasks/tools/) | talking to that cluster | any recent |
| [Helm](https://helm.sh) | installing the control plane chart | 3.x |
| a clone of [`pipe-cd/pipecd`](https://github.com/pipe-cd/pipecd) | **running piped itself** | — |

That last one is easy to miss: this repo is the *plugin*, not the CD agent that loads it. Piped
(`pipedv1`) is alpha and isn't distributed as a downloadable release binary yet, so running it locally
means building it from the `pipecd` source tree with `go run` / `make run/piped`. Clone it next to this
repo:

```sh
git clone https://github.com/pipe-cd/pipecd ~/pipecd
```

**Memory note:** if your machine has 8&nbsp;GB of RAM or less, don't run `make run/pipecd` in that
repo — it compiles the control plane's Go binary *and* its web console inside a Docker build, which can
exhaust a small VM and crash the Docker daemon. Steps below install the control plane from its
published Helm chart instead, which just pulls an image.

## 1. Bring up Kafka, a Kubernetes cluster, and the control plane

From this repo:

```sh
./hack/local-env.sh up
```

This one command:
- builds the plugin binary into `~/.piped/plugins/kafka`
- starts Redpanda (Kafka-API-compatible) and a schema registry via `docker compose`
- creates a plain `kind` cluster
- installs the PipeCD control plane from its published chart (`oci://ghcr.io/pipe-cd/chart/pipecd`) —
  no local build

It's idempotent — safe to re-run if a step fails partway, and `./hack/local-env.sh status` reports what's
currently up.

When it finishes, forward the control plane to your machine and leave this running in its own terminal:

```sh
kubectl -n pipecd port-forward svc/pipecd 8080
```

Open `http://localhost:8080` — project `quickstart`, user `hello-pipecd`, password `hello-pipecd`.

## 2. Register a piped

**Settings → Piped → +ADD.** Name it anything, save, and copy the **Piped ID** and the **base64-encoded
key** it shows you — that dialog only shows the key once.

## 3. Give piped somewhere to read topic definitions from

Piped reads desired state from a git repo — it's never pushed to, only polled. For a local test, a bare
repo on disk works exactly like a GitHub remote would:

```sh
rm -rf ~/kafka-demo.git ~/kafka-demo
git init -q --bare ~/kafka-demo.git

cp -R examples/simple/. ~/kafka-demo
cd ~/kafka-demo
git init -q -b main
git add -A && git commit -qm "Kafka topics for the orders service"
git remote add origin ~/kafka-demo.git
git push -q origin main
```

(To use a real GitHub repo instead, just push there and use its URL as `remote:` below — nothing else
changes.)

Now write the piped config. Replace the `pipedID` and `pipedKeyData` with what step 2 gave you:

```yaml
# ~/piped-config.yaml
apiVersion: pipecd.dev/v1beta1
kind: Piped
spec:
  projectID: quickstart
  pipedID: <from step 2>
  pipedKeyData: <from step 2>
  apiAddress: localhost:8080
  syncInterval: 1m
  repositories:
    - repoId: kafka
      remote: file:///Users/<you>/kafka-demo.git   # note the file:// scheme, see Troubleshooting
      branch: main
  plugins:
    - name: kafka
      port: 7020
      url: file:///Users/<you>/.piped/plugins/kafka
      deployTargets:
        - name: local
          config:
            bootstrapServers: ["localhost:9092"]
            schemaRegistry:
              url: http://localhost:8081
            allowTopicDeletion: false
            allowPartitionIncrease: false
```

Then, from your `pipecd` checkout:

```sh
cd ~/pipecd
make run/piped CONFIG_FILE=~/piped-config.yaml EXPERIMENTAL=true INSECURE=true
```

First run downloads a lot of Go modules and takes a few minutes. It's running once you see lines like
`found out 1 valid unregistered applications in repository "kafka"`.

## 4. Register the application

Back in the UI: **Applications → +ADD → ADD FROM SUGGESTIONS**. Piped will have found the app —
select it, and set:

- Piped: your piped
- **Deploy target: `local - kafka`** — easy to miss, nothing works without it
- Path: `orders`, Config Filename: `app.pipecd.yaml`

**SAVE**, open the application, click **SYNC**.

## 5. Watch it deploy

Open the deployment and click into the `KAFKA_PLAN` stage. You should see something like:

```
Plan: 2 to create topic, 1 to register schema.

  + create topic orders (12 partitions, replication factor 1)
  + create topic payments (6 partitions, replication factor 1)
  S register the first version of subject orders-value (topic orders)

The plan is applicable: 3 change(s) to make.
```

Then `KAFKA_REGISTER_SCHEMA` and `KAFKA_APPLY` run. Confirm the topics landed:

```sh
docker exec kafka-plugin-redpanda rpk topic list
```

## 6. The demo that actually matters: the safety gate

This is the behavior worth showing anyone — the same commit produces a different outcome depending on
what the *cluster* has opted into, not what the application asks for.

**Try an irreversible change.** Bump the partition count past what's currently allowed:

```sh
cd ~/kafka-demo
sed -i '' 's/partitions: 12/partitions: 24/' orders/topics/orders.yaml
git commit -aqm "Scale orders to 24 partitions" && git push -q origin main
```

Hit SYNC. `KAFKA_PLAN` should fail, **before anything is touched**:

```
Blocked (1). Nothing will be applied until these are resolved:
  x topic orders would grow from 12 to 24 partitions, but allowPartitionIncrease is
    false on this deploy target. This cannot be undone, and it changes which partition
    a key hashes to
```

`rpk topic describe orders` still shows 12 partitions.

**Now permit it.** Edit `~/piped-config.yaml`, set `allowPartitionIncrease: true` under the `local`
deploy target, stop piped (Ctrl-C) and restart the same `make run/piped` command. SYNC the *same*
commit again. `KAFKA_PLAN` now succeeds, but says plainly what it's about to do:

```
1 change(s) in this plan cannot be undone by a rollback. They are permitted by this
deploy target, so the deployment will continue.
```

`rpk topic describe orders` now shows 24. Same application, same commit — the deploy target's policy
was the only thing that changed.

## Tearing down

```sh
./hack/local-env.sh down     # stops Redpanda, deletes the kind cluster
```

Ctrl-C both the `port-forward` and `make run/piped` terminals.

## Troubleshooting

**`Failed to make GitPath URL`** when registering the application. The control plane only accepts
these remote schemes: `ssh`, `git`, `git+ssh`, `http`, `https`, `rsync`, `file`. A bare filesystem path
like `/Users/you/kafka-demo.git` has no scheme and is rejected — use `file:///Users/you/kafka-demo.git`.

**The application never shows up under ADD FROM SUGGESTIONS**, or piped logs
`invalid ApplicationInfo.Path: value does not match regex pattern "^[^/].+$"`. The application config
is sitting at the repository root. PipeCD requires it to live in a subdirectory — that's why
`examples/simple` nests everything under `orders/` rather than the repo root.

**Docker crashes or the control-plane install hangs** on a small machine. Don't run `make run/pipecd`
in the `pipecd` repo — see the memory note above. If Docker Desktop is already wedged, fully quit it
(`pkill -f "Docker Desktop"`, `pkill -f com.docker.backend`) rather than just closing the window, then
relaunch; a half-killed session can leave the engine unreachable even though the app appears to be
running.

**The deployment page shows no stage timeline**, even though the deployment says SUCCESS or FAILURE.
This is a version-skew issue between a `pipedv1` built from the tip of `pipecd`'s development branch
and a control-plane chart pinned to an older release — cosmetic, not a sign anything is broken. You can
always pull the real stage log directly:

```sh
kubectl exec -n pipecd deploy/pipecd-minio -- \
  find /data/quickstart/log -iname '*.txt'   # find the log object for your deployment/stage IDs
```
