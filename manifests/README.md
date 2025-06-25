# Implementing Kubernetes

## Setting up the Kubernetes **Cluster** (on WSL/Linux)

You can set up a cluster automatically using *k3s*: `curl -sfL https://get.k3s.io | sh -`. The script installs and writes a kubeconfig file on `/etc/rancher/k3s/k3s.yaml`, which you can copy to a local context on `$HOME/.kube/config` (*i.e*, dispensing the need to run `k3s` on every command) with:
```bash
# Every command in k3s requires sudo, kind of unnecessary
sudo k3s kubectl config view > ~/.kube/config
# You should now export kubeconfig to your environment variables (~/.profile)
echo "export KUBECONFIG=~/.kube/config" >> ~/.profile
```

> Note that this requires standard Kubernetes installed for *kubectl* to work.

Log out and back in your machine, cli should be working as intended. 
In order to reset the cluster, you need to restart the process with `systemctl stop k3s` and `systemctl start k3s`.
## Creating Deployments and Services
### From generated YAML
1. Check and fill the manifests informations, like port and other info. 
2. Run-and-forget the root manifests with the recursive flag: `kubectl apply -R -f manifests`
- OR, Apply each manifest with `kubectl apply -n=ingestion-ns -f MANIFEST_NAME.yml`. 
    - Manifests can be merged by simply adding `---` in a newline and continuing from below, requiring only one apply command.
Note: running `kubectl apply` after `kubectl create` **without** the `--save-config` flag will remind you to either save-config next time creating an object, or to apply instead of the former.


### Step-by-step guide
First and foremost, every command in kubernetes can be *dry-run*, which means you can test what would the output of a CLI command be and even check if the Kubernetes API Server (on the control plane) validates said output. For example, `kubectl create deploy TESTApp --image=registry.k8s.io/e2e-test-images/agnhost:2.39 --dry-run=client -o yaml` writes the output in yaml format:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  creationTimestamp: null
  labels:
    app: TESTApp
  name: TESTApp
spec:
  replicas: 1
  selector:
    matchLabels:
      app: TESTApp
  strategy: {}
  template:
    metadata:
      creationTimestamp: null
      labels:
        app: TESTApp
    spec:
      containers:
      - image: registry.k8s.io/e2e-test-images/agnhost:2.39
        name: agnhost
        resources: {}
status: {}
```
**TIP:** You can also write that to a file by appending `> object.yaml`. Basically every component can be created or applied, but should be applied from now on.
#### Deployments
If you want to do this manually, for each application you wish to break down into images and deploy, run `kubectl create deploy APPNAME --save-config -n=ingestion-ns --port=PORT --image=registry.k8s.io/e2e-test-images/agnhost:2.39`. Consider `--port` to be the port in which your application listens to incoming traffic. If you wish to generate a file (i.e, a manifest), append `-o yaml > FILENAME.yaml`. 

For this example, [agnhost](https://pkg.go.dev/k8s.io/kubernetes/test/images/agnhost#section-readme) is a general image used for debugging related topics. For redundancy (which is great contextually), add `--replicas=NUMBER_OF_REPLICAS`.
#### Environment Variables
If you wish to set environment variables, use `kubectl set env deployment/APPNAME KEY=VALUE` after creation, but I recommend changing later to a envFROM.
#### Services
Services can be created with `kubectl create service TYPE NAME --save-config -n=ingestion-ns --tcp=PORT:PORT`. You can reuse the `--tcp` flag to expose many ports. Check the help flag for more details.

Services can reference the container ports using either the port (for exposing on a service) or targetPort (for routing traffic to the container) fields. Remember targetPort is the port on the CONTAINER, the one it declares to use!