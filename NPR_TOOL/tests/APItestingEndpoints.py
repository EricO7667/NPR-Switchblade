import json
import requests
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import quote


# ============================================================
# CONFIG FLAGS
# Toggle these on/off depending on what you want fetched
# ============================================================
FETCH_PRODUCT_DETAILS = True
FETCH_SUBSTITUTIONS = True
FETCH_ALTERNATE_PACKAGING = True

USE_KEYWORD_SEARCH_FALLBACK = False


@dataclass
class DigiKeyFetchConfig:
    fetch_product_details: bool = FETCH_PRODUCT_DETAILS
    fetch_substitutions: bool = FETCH_SUBSTITUTIONS
    fetch_alternate_packaging: bool = FETCH_ALTERNATE_PACKAGING
    use_keyword_search_fallback: bool = USE_KEYWORD_SEARCH_FALLBACK


class JsonNode:
    def __init__(self, data: Any):
        object.__setattr__(self, "_data", data)

    @property
    def raw(self) -> Any:
        return object.__getattribute__(self, "_data")

    def get(self, key: Any, default: Any = None) -> Any:
        data = self.raw
        if isinstance(data, dict):
            value = data.get(key, default)
            return self._wrap(value)
        return default

    def pretty(self, indent: int = 4) -> str:
        return json.dumps(self.raw, indent=indent, ensure_ascii=False)

    def __getitem__(self, key: Any) -> Any:
        data = self.raw
        if isinstance(data, dict):
            return self._wrap(data[key])
        if isinstance(data, list):
            return self._wrap(data[key])
        raise TypeError(f"{type(data).__name__} does not support indexing.")

    def __getattr__(self, name: str) -> Any:
        data = self.raw
        if isinstance(data, dict) and name in data:
            return self._wrap(data[name])
        raise AttributeError(f"{name!r} not found.")

    def __iter__(self):
        data = self.raw
        if isinstance(data, list):
            for item in data:
                yield self._wrap(item)
        elif isinstance(data, dict):
            for key in data:
                yield key
        else:
            raise TypeError(f"{type(data).__name__} is not iterable.")

    def __len__(self) -> int:
        data = self.raw
        if isinstance(data, (dict, list, tuple, str)):
            return len(data)
        raise TypeError(f"{type(data).__name__} has no len().")

    def __repr__(self) -> str:
        data = self.raw
        if isinstance(data, dict):
            return f"JsonNode(dict keys={list(data.keys())[:8]})"
        if isinstance(data, list):
            return f"JsonNode(list len={len(data)})"
        return f"JsonNode({data!r})"

    @classmethod
    def _wrap(cls, value: Any) -> Any:
        if isinstance(value, (dict, list)):
            return cls(value)
        return value


class DigiKeyPayload(JsonNode):
    @classmethod
    def from_json(cls, data: dict) -> "DigiKeyPayload":
        return cls(data)

    @property
    def is_productdetails(self) -> bool:
        return isinstance(self.raw, dict) and "Product" in self.raw

    @property
    def is_keywordsearch(self) -> bool:
        return isinstance(self.raw, dict) and "Products" in self.raw

    @property
    def product(self) -> Optional[JsonNode]:
        if self.is_productdetails:
            return self.Product
        return None

    @property
    def products(self) -> list:
        if self.is_keywordsearch:
            return list(self.Products)
        if self.is_productdetails and self.product is not None:
            return [self.product]
        return []

    @property
    def primary_product(self) -> Optional[JsonNode]:
        if self.is_productdetails:
            return self.product
        prods = self.products
        return prods[0] if prods else None

    def parameter_map(self) -> dict[str, Any]:
        p = self.primary_product
        if not p:
            return {}

        result = {}
        params = p.get("Parameters", [])
        if isinstance(params, JsonNode):
            for param in params:
                if isinstance(param, JsonNode):
                    key = param.get("ParameterText")
                    val = param.get("ValueText")
                    if key is not None:
                        result[str(key)] = val
        return result

    def summary(self) -> dict[str, Any]:
        p = self.primary_product
        if not p:
            return {}

        desc = p.get("Description", {})
        manu = p.get("Manufacturer", {})
        clsf = p.get("Classifications", {})

        desc_raw = desc.raw if isinstance(desc, JsonNode) else {}
        manu_raw = manu.raw if isinstance(manu, JsonNode) else {}
        clsf_raw = clsf.raw if isinstance(clsf, JsonNode) else {}

        return {
            "manufacturer_part_number": p.get("ManufacturerProductNumber"),
            "manufacturer_name": manu_raw.get("Name"),
            "product_description": desc_raw.get("ProductDescription"),
            "detailed_description": desc_raw.get("DetailedDescription"),
            "quantity_available": p.get("QuantityAvailable"),
            "unit_price": p.get("UnitPrice"),
            "product_url": p.get("ProductUrl"),
            "datasheet_url": p.get("DatasheetUrl"),
            "photo_url": p.get("PhotoUrl"),
            "rohs_status": clsf_raw.get("RohsStatus"),
            "reach_status": clsf_raw.get("ReachStatus"),
            "parameters": self.parameter_map(),
        }


class DigiKeyProductDetailsPayload(DigiKeyPayload):
    pass


class DigiKeySubstitutionsPayload(DigiKeyPayload):
    @property
    def substitutions(self) -> list:
        data = self.raw
        if isinstance(data, dict):
            if "Products" in data:
                return list(self.Products)
            if "Substitutions" in data:
                return list(self.Substitutions)
        return []

    def count(self) -> int:
        return len(self.substitutions)


class DigiKeyAlternatePackagingPayload(DigiKeyPayload):
    @property
    def alternates(self) -> list:
        data = self.raw
        if isinstance(data, dict):
            if "Products" in data:
                return list(self.Products)
            if "AlternatePackaging" in data:
                return list(self.AlternatePackaging)
        return []

    def count(self) -> int:
        return len(self.alternates)


class DigiKeyPartBundle:
    """
    Owns the complete result set for one requested part lookup.
    """

    def __init__(
        self,
        requested_part_number: str,
        resolved_product_number: Optional[str] = None,
        product_details: Optional[DigiKeyProductDetailsPayload] = None,
        substitutions: Optional[DigiKeySubstitutionsPayload] = None,
        alternate_packaging: Optional[DigiKeyAlternatePackagingPayload] = None,
        config: Optional[DigiKeyFetchConfig] = None,
    ):
        self.requested_part_number = requested_part_number
        self.resolved_product_number = resolved_product_number
        self.product_details = product_details
        self.substitutions = substitutions
        self.alternate_packaging = alternate_packaging
        self.config = config or DigiKeyFetchConfig()

    @property
    def has_product_details(self) -> bool:
        return self.product_details is not None

    @property
    def has_substitutions(self) -> bool:
        return self.substitutions is not None

    @property
    def has_alternate_packaging(self) -> bool:
        return self.alternate_packaging is not None

    @property
    def primary_product(self) -> Optional[JsonNode]:
        if self.product_details:
            return self.product_details.primary_product
        return None

    def summary(self) -> dict[str, Any]:
        return {
            "requested_part_number": self.requested_part_number,
            "resolved_product_number": self.resolved_product_number,
            "has_product_details": self.has_product_details,
            "has_substitutions": self.has_substitutions,
            "has_alternate_packaging": self.has_alternate_packaging,
            "product_details_summary": self.product_details.summary() if self.product_details else None,
            "substitution_count": self.substitutions.count() if self.substitutions else 0,
            "alternate_packaging_count": self.alternate_packaging.count() if self.alternate_packaging else 0,
        }

    def pretty(self) -> str:
        return json.dumps(self.summary(), indent=4, ensure_ascii=False)

    def to_dict(self) -> dict[str, Any]:
        return {
            "requested_part_number": self.requested_part_number,
            "resolved_product_number": self.resolved_product_number,
            "product_details": self.product_details.raw if self.product_details else None,
            "substitutions": self.substitutions.raw if self.substitutions else None,
            "alternate_packaging": self.alternate_packaging.raw if self.alternate_packaging else None,
        }


class DigiKeyClient:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        site: str = "US",
        language: str = "en",
        currency: str = "USD",
        production: bool = True,
        timeout: int = 30,
        config: Optional[DigiKeyFetchConfig] = None,
    ):
        self.client_id = str(client_id).strip()
        self.client_secret = str(client_secret).strip()
        self.site = site
        self.language = language
        self.currency = currency
        self.timeout = timeout
        self.base = "https://api.digikey.com" if production else "https://sandbox-api.digikey.com"
        self._token: Optional[str] = None
        self.config = config or DigiKeyFetchConfig()

    def _token_url(self) -> str:
        return f"{self.base}/v1/oauth2/token"

    def _headers(self, include_content_type: bool = False) -> dict:
        headers = {
            "Authorization": f"Bearer {self._token}",
            "X-DIGIKEY-Client-Id": self.client_id,
            "X-DIGIKEY-Locale-Site": self.site,
            "X-DIGIKEY-Locale-Language": self.language,
            "X-DIGIKEY-Locale-Currency": self.currency,
            "Accept": "application/json",
        }
        if include_content_type:
            headers["Content-Type"] = "application/json"
        return headers

    def authenticate(self) -> None:
        response = requests.post(
            self._token_url(),
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            },
            data={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials",
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        token = payload.get("access_token")
        if not token:
            raise RuntimeError("No access_token returned from DigiKey.")
        self._token = token

    def ensure_authenticated(self) -> None:
        if not self._token:
            self.authenticate()

    def product_details(self, part_number: str) -> DigiKeyProductDetailsPayload:
        self.ensure_authenticated()
        encoded = quote(part_number.strip(), safe="")
        response = requests.get(
            f"{self.base}/products/v4/search/{encoded}/productdetails",
            headers=self._headers(),
            timeout=self.timeout,
        )
        response.raise_for_status()
        return DigiKeyProductDetailsPayload.from_json(response.json())

    def keyword_search(self, keyword: str, record_count: int = 10) -> DigiKeyPayload:
        self.ensure_authenticated()
        response = requests.post(
            f"{self.base}/products/v4/search/keyword",
            headers=self._headers(include_content_type=True),
            json={
                "Keywords": keyword.strip(),
                "RecordCount": record_count,
                "RecordStartPosition": 0,
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        return DigiKeyPayload.from_json(response.json())

    def substitutions(self, product_number: str) -> DigiKeySubstitutionsPayload:
        self.ensure_authenticated()
        encoded = quote(product_number.strip(), safe="")
        response = requests.get(
            f"{self.base}/products/v4/search/{encoded}/substitutions",
            headers=self._headers(),
            timeout=self.timeout,
        )
        response.raise_for_status()
        return DigiKeySubstitutionsPayload.from_json(response.json())

    def alternate_packaging(self, product_number: str) -> DigiKeyAlternatePackagingPayload:
        self.ensure_authenticated()
        encoded = quote(product_number.strip(), safe="")
        response = requests.get(
            f"{self.base}/products/v4/search/{encoded}/alternatepackaging",
            headers=self._headers(),
            timeout=self.timeout,
        )
        response.raise_for_status()
        return DigiKeyAlternatePackagingPayload.from_json(response.json())

    def resolve_product_number(self, requested_part_number: str) -> str:
        """
        Ownership rule:
        - if ProductDetails succeeds, use the requested part number as the endpoint identifier
        - if you later want stricter DigiKey-number resolution, patch this method only
        """
        return requested_part_number.strip()

    def build_part_bundle(
        self,
        requested_part_number: str,
        config: Optional[DigiKeyFetchConfig] = None,
    ) -> DigiKeyPartBundle:
        cfg = config or self.config
        requested_part_number = requested_part_number.strip()

        product_details_payload = None
        substitutions_payload = None
        alternate_packaging_payload = None
        resolved_product_number = None

        if cfg.fetch_product_details:
            product_details_payload = self.product_details(requested_part_number)
            resolved_product_number = self.resolve_product_number(requested_part_number)

        if not resolved_product_number and cfg.use_keyword_search_fallback:
            search_payload = self.keyword_search(requested_part_number, record_count=1)
            first_product = search_payload.primary_product
            if first_product:
                resolved_product_number = (
                    first_product.get("DigiKeyPartNumber")
                    or first_product.get("DigiKeyProductNumber")
                    or requested_part_number
                )

        if not resolved_product_number:
            resolved_product_number = requested_part_number

        if cfg.fetch_substitutions:
            substitutions_payload = self.substitutions(resolved_product_number)

        if cfg.fetch_alternate_packaging:
            alternate_packaging_payload = self.alternate_packaging(resolved_product_number)

        return DigiKeyPartBundle(
            requested_part_number=requested_part_number,
            resolved_product_number=resolved_product_number,
            product_details=product_details_payload,
            substitutions=substitutions_payload,
            alternate_packaging=alternate_packaging_payload,
            config=cfg,
        )


# ============================================================
# EXAMPLE USAGE
# ============================================================
if __name__ == "__main__":
    config = DigiKeyFetchConfig(
        fetch_product_details=True,
        fetch_substitutions=True,
        fetch_alternate_packaging=True,
        use_keyword_search_fallback=False,
    )

    client = DigiKeyClient(
        client_id="XXXX",
        client_secret="YYY",
        production=True,
        config=config,
    )

    bundle = client.build_part_bundle("490-4786-2-ND")

    print(bundle.pretty())

    if bundle.product_details:
        print(bundle.product_details.summary())

    if bundle.substitutions:
        print("Substitution count:", bundle.substitutions.count())

    if bundle.alternate_packaging:
        print("Alternate packaging count:", bundle.alternate_packaging.count())