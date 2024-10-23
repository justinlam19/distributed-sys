### UIUD

A UUID is a 128-bit number that acts as a unique identifier. It is separated into 5 groups separated by hyphens. Depending on the version, UUIDs are generated in different ways.

Some common versions include datetime and MAC address, namespace hash, or even random generation. In general UUIDs don't actually guarantee a 0 probability of collision, but their generation method and format ensures that the chance of collision is extremely low.

For the datetime and MAC address method, the probability that two UUIDs are generated on the same MAC address at the same datetime is vanishingly low. For namespace hashing, as long as the namespace is unique and the hash function maps to sufficiently many hashes, the collision probability is low. For random generation, it relies on the large space of possible UUIDs (2^128) to keep collision probability low.


### B1

Prediction: the pod gets deleted and the service ends.

Actual reality: after the pod gets deleted, a new pod is created. I think this is because kubernetes is managing the pods so that when a pod goes down, a new instance is created to make sure the service keeps running. This ensures that the service remains available after failures occur.


### B4

After going to `lab2.cs426.cloud/recommend/jl4268/video-rec/jl4268/`, I get:

```
Welcome! You have chosen user ID 203264 (Lesch4479/raeganadams@koss.org)

Their recommended videos are:
 1. The lingering rhinoceros's water by Reina Mueller
 2. drab above by Lynn Graham
 3. The careful clam's management by Nadia Stracke
 4. The repelling skunk's calm by Dennis Cummerata
 5. The white guinea pig's relaxation by Adrain Corwin
```


### C3

For a client pool size of 4, the load is distributed between the two backing pods, which is expected behavior for round robin. It is not perfectly evenly distributed, because the distribution depends on the distribution of pods connected to during server creation.

For a size of 1, I expect the load to be concentrated in a single backing pod, like the previous requests when the client pool hadn't been implemented yet. 

For a size of 8, I expect it to behave similar to size 4, where the load is distributed between the two backing pods. This is because there's only 2 backing pods, so any client pool size of 2 or above will generally exhibit the same behavior.

The above predictions were confirmed when I redeployed with different client pool sizes and tested loadgen.
