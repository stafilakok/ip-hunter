# Yandex Cloud IP Hunter

Local Python utility that reserves public static IPv4 addresses in Yandex Cloud
and checks whether the allocated address belongs to configured IPs or CIDR
pools. It supports two explicit rotation modes:

- `folder`: create a fresh folder inside one target cloud for each attempt.
- `cloud`: create a fresh cloud for each attempt, bind billing, try an IP, then
  submit async cloud deletion on miss.

This is destructive when live mode is enabled. Keep the service account in a
stable cloud that this script will never delete.

## What it uses

- IAM token endpoint: `POST https://iam.api.cloud.yandex.net/iam/v1/tokens`
- VPC address endpoint: `POST https://vpc.api.cloud.yandex.net/vpc/v1/addresses`
- Folder/cloud create/delete endpoints under Resource Manager
- Billing bind endpoint:
  `POST /billing/v1/billingAccounts/{billingAccountId}/billableObjectBindings`
- Operation polling:
  `GET https://operation.api.cloud.yandex.net/operations/{operationId}`

## Requirements

- Python 3.9+
- For service account key auth:

```powershell
python -m pip install PyJWT cryptography
```

No extra dependency is needed if you pass an IAM token through the `YC_IAM_TOKEN`
environment variable.

The service account needs enough permissions to:

- folder mode: admin on the target cloud;
- cloud mode: organization-level cloud create/delete rights and billing account
  binding rights;
- both modes: create folders and VPC public addresses.

## Setup

1. Copy `config.example.json` to `config.json`.
2. Fill in `organization_id`, `billing_account_id`, and either:
   - `auth.service_account_key_file`, or
   - `YC_IAM_TOKEN` environment variable.
3. For folder mode, set `rotation_mode=folder` and `target_cloud_id`.
4. For cloud mode, set `rotation_mode=cloud`, `organization_id`,
   `billing_account_id`, and `service_cloud_id`.
5. Keep `dry_run=true` for the first check.

Run a dry run:

```powershell
python .\yc_ip_hunter.py --config .\config.json --dry-run
```

Run live in folder mode:

```powershell
python .\yc_ip_hunter.py --config .\config.json --run
```

Run live in cloud mode:

```powershell
python .\yc_ip_hunter.py --config .\config.json --run --yes-delete-cloud
```

For live cloud mode, the config must also contain:

```json
{
  "rotation_mode": "cloud",
  "allow_delete_cloud": true,
  "immediate_delete_cloud": true,
  "max_parallel_clouds": 4,
  "dry_run": false
}
```

## Candidate selection

By default, `allocation_mode` is `random`: Yandex Cloud allocates any available
public IPv4 in a zone, and the script checks whether that IP is in `target_ips`
or `target_cidrs`. Unmatched addresses are deleted when
`delete_unmatched_addresses=true`.

There is also `allocation_mode=specific`, where the script asks the API for an
exact address. In many accounts Yandex Cloud rejects this with `Permission denied
to create specific address`; keep `random` unless your account is explicitly
allowed to reserve exact public IPs.

`max_iterations` limits how many rotation attempts are made. Set it to `0` to
run continuously until a target IP is found. In cloud mode the script checks
`max_parallel_clouds` before creating a new cloud and waits rather than
hammering the API when the organization cloud quota is full.

In cloud mode, `max_addresses_per_cloud` controls how many public IPs are tried
inside one cloud before the cloud is deleted. The working default is `9`, with
`address_iteration_sleep_seconds` between address attempts to avoid the 10th
allocation hitting the VPC creation limit.

## State and logs

- `state.json` stores current cloud/folder, attempts, and success result.
- `run.log` stores all REST operations and decisions.

Delete `state.json` only when you intentionally want the script to forget its
current cloud/folder and previous attempts.

## Notes

`deleteAfter=1970-01-01T00:00:00Z` plus non-waiting delete starts deletion
immediately, but it does not guarantee instant quota release. Deleting clouds can
still leave them counted until Yandex Cloud finishes cleanup.
