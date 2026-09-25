# 09 — Kubernetes & GitOps

## What you'll learn
- How the Compose services map to Kubernetes objects (Deployment / StatefulSet / Service / HPA).
- **Kustomize** bases and overlays — one source of truth, per-environment tweaks.
- **GitOps** with ArgoCD: the cluster syncs itself from Git, no manual `kubectl apply`.
- Why stateless vs stateful workloads are deployed differently.

> Scope note: these manifests show the **shape** of a production deployment and run on
> Minikube. Provisioning real cloud VMs/clusters (Terraform + Ansible in the plan) is out of
> scope — [ADR-0001](decisions/0001-deviations-from-plan.md) explains why.

## From Compose to Kubernetes

| Compose service | K8s object(s) | Why |
|-----------------|---------------|-----|
| ingestion | Deployment + Service + **HPA** | stateless → replicate + autoscale |
| processor | Deployment | stateless *per partition*; scale via Kafka consumer group |
| dashboard | Deployment + Service + **Ingress** | stateless; Ingress terminates the WebSocket |
| redpanda | **StatefulSet** + headless Service | stable identity + persistent volume |
| timescaledb | **StatefulSet** + PVC + init ConfigMap | durable data, stable network id |
| redis | Deployment + Service | ephemeral (Pub/Sub only) → no PVC needed |

Files: [`deploy/k8s/base/`](../deploy/k8s/base/).

### Stateless vs stateful — the key distinction

- **Stateless** services (ingestion, processor, dashboard) hold no durable data. Any replica
  can handle any request, so a `Deployment` (interchangeable pods) is right, and you scale by
  changing `replicas` or letting an HPA do it.
- **Stateful** services (Redpanda, TimescaleDB) own data on disk and need a **stable identity**
  (`redpanda-0`, its own volume). That's a `StatefulSet` with `volumeClaimTemplates`, which
  gives each pod a persistent PVC that survives restarts.

### Scaling the processor is special

The processor is a Deployment, but its scaling is governed by **Kafka**, not an HPA. All
replicas join the consumer group `fleet-processor`; Kafka assigns the 6 `gps-pings` partitions
across them. Because pings are keyed by `vehicle_id`, each vehicle sticks to one partition →
one replica, so the in-memory "last position per vehicle" state stays correct with **no shared
store**. Useful parallelism therefore caps at the partition count (6) — to go higher, add
partitions. This is why partitioning ([03](03-ingestion.md)) was a deliberate design choice,
not an afterthought.

### The DB init trick

The same `db/init/*.sql` that seeds Compose is turned into a **ConfigMap** by Kustomize
(`configMapGenerator`) and mounted into TimescaleDB's `/docker-entrypoint-initdb.d`. One source
of schema, two runtimes.

## Kustomize: base + overlays

Instead of copy-pasting YAML per environment, Kustomize layers changes:

```
deploy/k8s/
├── base/              # the full app, sensible defaults (1 replica each)
│   └── kustomization.yaml
└── overlays/
    ├── dev/           # = base (seam for dev-only tweaks)
    └── prod/          # patches: more replicas, higher HPA ceiling
```

Render and inspect what would be applied:

```bash
kubectl kustomize deploy/k8s/overlays/dev
kubectl kustomize deploy/k8s/overlays/prod   # see the replica patches applied
```

Apply directly (without GitOps):

```bash
kubectl apply -k deploy/k8s/overlays/dev
```

The `images:` transformer in the base sets the image tag in one place; CI overrides it with the
real registry + immutable tag per build.

## GitOps with ArgoCD

**Without GitOps**, deploying means a human (or CI) running `kubectl apply` — imperative, easy to
drift, hard to audit. **With GitOps**, Git is the single source of truth and a controller in the
cluster continuously makes reality match Git.

[`deploy/argocd/application.yaml`](../deploy/argocd/application.yaml) declares one ArgoCD
`Application`:

```yaml
source:
  repoURL: https://github.com/1morshed1/fleet-telemetry.git
  targetRevision: main
  path: deploy/k8s/overlays/dev
destination: { server: https://kubernetes.default.svc, namespace: fleet }
syncPolicy:
  automated: { prune: true, selfHeal: true }
```

The flow:

1. You apply this **one** manifest into the `argocd` namespace.
2. ArgoCD watches `deploy/k8s/overlays/dev` on `main`.
3. **CI** builds the image, pushes it, and updates the image tag in Git (`kustomize edit set
   image`). That commit is the deploy.
4. ArgoCD notices the Git change and syncs the cluster to match.
5. `selfHeal: true` reverts any manual `kubectl` change back to Git; `prune: true` deletes
   resources removed from Git.
6. Rollback = `git revert`. Your deploy history *is* your Git history.

In a real setup the K8s manifests live in a **separate** GitOps repo from the app code (so an
image-tag bump doesn't rebuild the app, and access can be scoped differently). We keep them
in-repo under `deploy/k8s` so the whole project is one clone to learn from —
[ADR-0001](decisions/0001-deviations-from-plan.md).

## Try it on Minikube

```bash
minikube start
eval $(minikube docker-env)          # build into minikube's docker
docker build -t fleet-telemetry:latest .
kubectl apply -k deploy/k8s/overlays/dev
kubectl -n fleet get pods
kubectl -n fleet port-forward svc/dashboard 8080:80
```

Install ArgoCD first if you want the GitOps path, then `kubectl apply -f deploy/argocd/application.yaml`.

## The takeaway

Describe **desired state** declaratively (Kustomize), keep it in **Git**, and let a controller
(ArgoCD) reconcile the cluster to it. Deploys become commits; rollbacks become reverts;
"what's running?" becomes "what's in Git?". That's the whole idea behind how large teams run
Kubernetes.
