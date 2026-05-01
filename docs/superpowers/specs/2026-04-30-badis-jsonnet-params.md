# Parametrize Kubernetes Jsonnet Configuration

**Goal:** Allow users to easily configure parameters like `namespace`, `replicas`, `proxyReplicas`, `volumeSize`, and `storageClass` when deploying Badis using the `kubecfg` jsonnet file.

## Approach: Top-Level Arguments (TLAs)

We will modify `k8s/badis.jsonnet` to return a function instead of a literal array. This is the idiomatic way in jsonnet to accept Top-Level Arguments. Users can supply these arguments using `--tla-str` or `--tla-code` flags with `kubecfg` or `jsonnet`.

### Design Changes

1.  **Function Wrapper:** Wrap the entire file in:
    ```jsonnet
    function(
      namespace='default',
      replicas=3,
      proxyReplicas=2,
      volumeSize='5Gi',
      storageClass=null
    )
    ```
2.  **StatefulSet VolumeClaimTemplates:** Update the `volumeClaimTemplates` to use the provided `volumeSize`.
3.  **Storage Class Handling:** If `storageClass` is provided (i.e., `!= null`), inject it into the `volumeClaimTemplates` via standard jsonnet conditional merging or just adding the field.
4.  **Local Variables Cleanup:** Remove the static `local` declarations at the top of the file that clash with the new function arguments.

### Trade-offs

*   **Pros:** Highly flexible. Users can easily import this file as a library in other jsonnet files and pass arguments, or use `kubecfg --tla-str`. Defaults are cleanly defined in the function signature.
*   **Cons:** Users invoking `kubecfg update k8s/badis.jsonnet` without any flags get the defaults automatically. Modifying via CLI requires somewhat verbose `--tla-str` flags.

## Example Usage

Once implemented, a user could deploy to a different namespace with a specific storage class using:

```bash
kubecfg update k8s/badis.jsonnet \
  --tla-str namespace=prod \
  --tla-code replicas=5 \
  --tla-str volumeSize=20Gi \
  --tla-str storageClass=fast-ssd
```