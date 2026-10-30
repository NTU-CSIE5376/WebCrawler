"""Load and resolve site policies shared by site-specific middleware."""
from dataclasses import dataclass
from functools import lru_cache
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator
from requests.utils import check_header_validity
from scrapy.http import Headers
from twisted.web.http_headers import Headers as HTTPHeaders
import tldextract

from libs.config.loader import load_yaml
from libs.urls import normalize_hostname


# Use the bundled suffix list, with no network fetches or cache writes.
DOMAIN_EXTRACTOR = tldextract.TLDExtract(suffix_list_urls=(), cache_dir=None)


def get_registered_domain(hostname: str) -> str:
    result = DOMAIN_EXTRACTOR(hostname)
    if hasattr(result, "top_domain_under_public_suffix"):
        return result.top_domain_under_public_suffix
    return result.registered_domain  # tldextract 5.1, also used by the API image.


class RequestLimits(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    download_slot: str | None = Field(default=None, min_length=1, pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
    max_concurrent_requests: int = Field(default=16, gt=0)
    max_qps: float = Field(default=4, gt=0, allow_inf_nan=False)

    @property
    def min_request_delay_sec(self):
        return 1 / self.max_qps


class SitePolicyOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    crawler_mode: Literal["autothrottle", "fixed"] | None = None
    middleware: str | None = Field(default=None, min_length=1)
    headers: dict[str, str | None] = Field(default_factory=dict)
    download_timeout_sec: float | None = Field(default=None, gt=0, allow_inf_nan=False)
    request_limits: RequestLimits | None = None

    @field_validator("headers")
    @classmethod
    def validate_headers(cls, headers):
        for name, value in headers.items():
            try:
                HTTPHeaders({name: []})  # Validate the HTTP field name using Twisted.
                check_header_validity((name, "" if value is None else value))
            except ValueError as error:
                raise ValueError(f"Invalid configured header: {name!r}") from error
        return {name.title(): value for name, value in headers.items()}


class DefaultSitePolicy(SitePolicyOptions):
    crawler_mode: Literal["autothrottle", "fixed"] = "autothrottle"
    request_limits: RequestLimits = Field(default_factory=RequestLimits)


class SitePoliciesConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    default: DefaultSitePolicy = Field(default_factory=DefaultSitePolicy)
    domains: dict[str, SitePolicyOptions] = Field(default_factory=dict)
    hosts: dict[str, SitePolicyOptions] = Field(default_factory=dict)

    @field_validator("domains", "hosts")
    @classmethod
    def normalize_policy_hostnames(cls, rules):
        normalized = {normalize_hostname(hostname): policy for hostname, policy in rules.items()}
        if len(normalized) != len(rules):
            raise ValueError("Duplicate normalized site policy hostname")
        return normalized

    @field_validator("domains")
    @classmethod
    def validate_registered_domains(cls, rules):
        for hostname in rules:
            if get_registered_domain(hostname) != hostname:
                raise ValueError(f"Domain policy must name a registered domain: {hostname!r}")
        return rules


@dataclass(frozen=True)
class SitePolicy:
    headers: dict
    download_timeout_sec: float | None = None
    middleware: str | None = None
    request_limits: RequestLimits | None = None
    crawler_mode: str = "autothrottle"


class SitePolicies:
    def __init__(self, document):
        self.config = SitePoliciesConfig.model_validate(document)
        self.download_slots()

    @classmethod
    def load(cls):
        return cls(load_yaml("config/site_policies.yml"))

    def download_slots(self, *, start_delay=0.5):
        """Configure slots for domain and host overrides in either mode."""
        slots = {}
        policies_by_slot = {}
        for hostname in (*self.config.domains, *self.config.hosts):
            policy = self.resolve(hostname)
            limits = policy.request_limits
            if limits.download_slot is None:
                continue
            delay = limits.min_request_delay_sec
            if policy.crawler_mode == "autothrottle":
                delay = max(delay, start_delay)
            settings = {"concurrency": limits.max_concurrent_requests, "delay": delay}
            previous = policies_by_slot.setdefault(limits.download_slot, (policy.crawler_mode, limits))
            if previous != (policy.crawler_mode, limits):
                raise ValueError(f"Conflicting request limits for shared slot {limits.download_slot!r}")
            slots[limits.download_slot] = settings
        return slots

    @lru_cache(maxsize=4096)
    def resolve(self, hostname):
        hostname = normalize_hostname(hostname)
        domain = get_registered_domain(hostname)
        default = self.config.default
        base = self.config.domains.get(domain, SitePolicyOptions())
        host = self.config.hosts.get(hostname, SitePolicyOptions())
        limits = default.request_limits
        for key, rule in ((domain, base), (hostname, host)):
            if "request_limits" in rule.model_fields_set and rule.request_limits is None:
                limits = default.request_limits
                if rule.crawler_mode is not None:
                    limits = limits.model_copy(update={"download_slot": key})
            elif rule.request_limits is not None:
                limits = limits.model_copy(update=rule.request_limits.model_dump(exclude_unset=True))
                if limits.download_slot is None or "download_slot" not in rule.request_limits.model_fields_set:
                    limits = limits.model_copy(update={"download_slot": key})
            elif rule.crawler_mode is not None:
                limits = limits.model_copy(update={"download_slot": key})
        headers = Headers({**default.headers, **base.headers, **host.headers})
        return SitePolicy(
            headers={key: tuple(values) for key, values in headers.items()},
            download_timeout_sec=next((rule.download_timeout_sec for rule in (host, base, default)
                                       if rule.download_timeout_sec is not None), None),
            middleware=next((rule.middleware for rule in (host, base, default)
                             if "middleware" in rule.model_fields_set), None),
            request_limits=limits,
            crawler_mode=host.crawler_mode or base.crawler_mode or default.crawler_mode,
        )
