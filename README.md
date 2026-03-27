# target-inriver

Singer target for [InRiver](https://www.inriver.com/) using [HotglueSingerSDK](https://github.com/hotgluexyz/HotglueSingerSDK) (`TargetHotglue`). The SDK is pulled from Git in `pyproject.toml` (same source as `target-dualentry`). To use a local checkout instead: `poetry add --editable ../path/to/HotglueSingerSDK`. Streams: **Product**, **ProductItem** (Item entity + `ProductItem` link), **ItemSize** (Size entity + `ItemSize` link, sizes deduped by `externalId` via bookmarks).

## Config

- `api_key` (required): `X-inRiver-APIKey`
- `base_url` or `api_url_base` (one required): API root, no trailing slash
- Optional: `stream_maps`, `stream_map_config` for [PluginMapper](https://sdk.meltano.com/en/latest/stream_maps.html) (Hotglue fork)

See [config.example.json](config.example.json).

## Run

```bash
cd /path/to/target-inriver
poetry install
poetry run target-inriver --config config.example.json --config /path/to/secrets.json < fixtures/data.singer
```

Use a real config with your API key. Records must arrive in order: **Product** → **ProductItem** → **ItemSize** (see [fixtures/data.singer](fixtures/data.singer)).

## Tenant validation

```bash
./scripts/tenant_getempty.sh .secrets/config.json
```

Compare required fields with your model (`Product`, `Item`, `Size`). See also [curl-roller-skates.txt](curl-roller-skates.txt).

## External IDs

- `TargetHotglue.get_record_id` resolves `productExternalId` / `itemExternalId` using **bookmarks** and Hotglue **snapshots** (no snapshot reads in this package’s sinks).
- **ItemSize** keeps `externalId` on the record (`allows_externalid`) as the stable size key (e.g. `RS-RED-ALPHA-S`).

### Snapshot CSVs (Hotglue)

Files follow `{StreamName}_{FLOW_ID}.snapshot.csv` under `SNAPSHOT_DIR` (see SDK). Columns match your Vendors/Bills samples: **`InputId`** (tap/external id, same as Singer `externalId`) and **`RemoteId`** (InRiver entity id).

Example copies for flow id `A8GglKB3a`: [fixtures/snapshots/](fixtures/snapshots/) (ids taken from a `state.json` run; adjust **RemoteId** if your tenant differs). Set env **`FLOW`** (or `FLOW_ID`) to match the filename suffix when testing.
