"""InRiver sinks: Product, ProductItem (Item + ProductItem link), ItemSize (Size + ItemSize link)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

from hotglue_singer_sdk.exceptions import FatalAPIError

from target_inriver.client import InRiverSink

# Keys not sent as InRiver fieldTypeId payloads
_META_KEYS: Set[str] = {
    "externalId",
    "id",
    "productExternalId",
    "itemExternalId",
}


def _entity_id_from_response(data: Any) -> Optional[str]:
    if not isinstance(data, dict):
        return None
    for key in ("entityId", "id", "Id", "ID"):
        val = data.get(key)
        if val is not None:
            return str(val)
    nested = data.get("entity")
    if isinstance(nested, dict):
        return _entity_id_from_response(nested)
    return None


def _field_values_from_record(record: Dict[str, Any], skip: Set[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for key, val in record.items():
        if key in skip or val is None:
            continue
        if isinstance(val, (dict, list)):
            continue
        out.append({"fieldTypeId": key, "value": val})
    return out


def _as_int(v: Any) -> int:
    if isinstance(v, int):
        return v
    return int(v)


def _duplicate_link_message(msg: str) -> bool:
    lower = msg.lower()
    return any(
        s in lower
        for s in (
            "duplicate",
            "already",
            "exists",
            "conflict",
            "409",
            "not unique",
        )
    )


class ProductSink(InRiverSink):
    """Product entity: createnew or PUT fieldvalues when `id` is set (e.g. from snapshots)."""

    name = "Product"

    @property
    def endpoint(self) -> str:
        return "/api/v1.0.1/entities:createnew"

    def preprocess_record(self, record: dict, context: dict) -> dict:
        return record

    def upsert_record(self, record: dict, context: dict):
        state: Dict[str, Any] = {}
        rid = record.get("id")
        fields = _field_values_from_record(record, _META_KEYS)

        if rid is not None:
            ep = f"/api/v1.0.0/entities/{_as_int(rid)}/fieldvalues"
            resp = self.request_api("PUT", endpoint=ep, request_data=fields)
            entity_id = str(_as_int(rid))
            state["is_updated"] = True
            return entity_id, resp.ok, state

        body = {"entityTypeId": "Product", "fieldValues": fields}
        resp = self.request_api(
            "POST",
            endpoint="/api/v1.0.1/entities:createnew",
            request_data=body,
        )
        entity_id = _entity_id_from_response(resp.json())
        return entity_id, resp.ok, state


class ProductItemSink(InRiverSink):
    """Item entity plus ProductItem link (product resolved via productExternalId -> get_record_id)."""

    name = "ProductItem"
    relation_fields = [{"field": "productExternalId", "objectName": "Product"}]

    @property
    def endpoint(self) -> str:
        return "/api/v1.0.1/entities:createnew"

    def preprocess_record(self, record: dict, context: dict) -> dict:
        return record

    def _post_product_item_link(self, product_entity_id: int, item_entity_id: int) -> None:
        body = {
            "linkTypeId": "ProductItem",
            "sourceEntityId": product_entity_id,
            "targetEntityId": item_entity_id,
            "isActive": True,
        }
        try:
            self.request_api("POST", endpoint="/api/v1.0.0/links", request_data=body)
        except FatalAPIError as err:
            if _duplicate_link_message(str(err)):
                self.logger.info(
                    "ProductItem link skipped (treat as existing): %s -> %s",
                    product_entity_id,
                    item_entity_id,
                )
                return
            raise

    def upsert_record(self, record: dict, context: dict):
        state: Dict[str, Any] = {}
        product_id = record.get("productExternalId")
        if product_id is None:
            raise FatalAPIError("productExternalId missing after relation resolution")

        fields = _field_values_from_record(record, _META_KEYS)
        rid = record.get("id")

        if rid is not None:
            ep = f"/api/v1.0.0/entities/{_as_int(rid)}/fieldvalues"
            resp = self.request_api("PUT", endpoint=ep, request_data=fields)
            item_entity_id = _as_int(rid)
            state["is_updated"] = True
        else:
            body = {"entityTypeId": "Item", "fieldValues": fields}
            resp = self.request_api(
                "POST",
                endpoint="/api/v1.0.1/entities:createnew",
                request_data=body,
            )
            parsed = resp.json()
            eid = _entity_id_from_response(parsed)
            if eid is None:
                raise FatalAPIError(f"Could not parse Item entity id from response: {parsed}")
            item_entity_id = int(eid)

        if not state.get("is_updated"):
            self._post_product_item_link(_as_int(product_id), item_entity_id)
        return str(item_entity_id), resp.ok, state


class ItemSizeSink(InRiverSink):
    """Size entity (deduped by externalId in bookmarks) and ItemSize link."""

    name = "ItemSize"
    allows_externalid = ["ItemSize"]
    relation_fields = [{"field": "itemExternalId", "objectName": "ProductItem"}]

    @property
    def endpoint(self) -> str:
        return "/api/v1.0.1/entities:createnew"

    def preprocess_record(self, record: dict, context: dict) -> dict:
        return record

    def _bookmark_size_id(self, external_id: str) -> Optional[str]:
        if not self.latest_state:
            return None
        for b in self.latest_state.get("bookmarks", {}).get(self.name, []):
            if b.get("externalId") == external_id and b.get("success") and b.get("id"):
                return str(b["id"])
        return None

    def _post_item_size_link(self, item_entity_id: int, size_entity_id: int) -> None:
        body = {
            "linkTypeId": "ItemSize",
            "sourceEntityId": item_entity_id,
            "targetEntityId": size_entity_id,
            "isActive": True,
        }
        try:
            self.request_api("POST", endpoint="/api/v1.0.0/links", request_data=body)
        except FatalAPIError as err:
            if _duplicate_link_message(str(err)):
                self.logger.info(
                    "ItemSize link skipped (treat as existing): item %s -> size %s",
                    item_entity_id,
                    size_entity_id,
                )
                return
            raise

    def upsert_record(self, record: dict, context: dict):
        state: Dict[str, Any] = {}
        item_id = record.get("itemExternalId")
        if item_id is None:
            raise FatalAPIError("itemExternalId missing after relation resolution")

        ext = record.get("externalId")
        if not ext:
            raise FatalAPIError("externalId required on ItemSize stream (stable size key)")

        size_skip = set(_META_KEYS)
        fields = _field_values_from_record(record, size_skip)
        rid = record.get("id")

        existing = self._bookmark_size_id(str(ext))
        if existing and rid is None:
            size_entity_id = int(existing)
            resp_ok = True
            self._post_item_size_link(_as_int(item_id), size_entity_id)
            return str(size_entity_id), resp_ok, state

        if rid is not None:
            ep = f"/api/v1.0.0/entities/{_as_int(rid)}/fieldvalues"
            resp = self.request_api("PUT", endpoint=ep, request_data=fields)
            size_entity_id = _as_int(rid)
            state["is_updated"] = True
        else:
            body = {"entityTypeId": "Size", "fieldValues": fields}
            resp = self.request_api(
                "POST",
                endpoint="/api/v1.0.1/entities:createnew",
                request_data=body,
            )
            parsed = resp.json()
            eid = _entity_id_from_response(parsed)
            if eid is None:
                raise FatalAPIError(f"Could not parse Size entity id from response: {parsed}")
            size_entity_id = int(eid)

        if not state.get("is_updated"):
            self._post_item_size_link(_as_int(item_id), size_entity_id)
        return str(size_entity_id), resp.ok, state
