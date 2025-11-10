#!/usr/bin/env python3
"""
Robust website people enricher.

Given a set of company websites, this script scans each site to discover team,
leadership, or people pages. It extracts team members, prioritises decision
makers, and writes the findings to a JSON file. The script is designed for
offline testing and does not touch Google Sheets.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import logging
import random
import re
import sys
import time
from dataclasses import dataclass, field
from typing import Iterable, List, Optional, Dict, Callable, Any
from urllib.parse import urljoin, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup, Tag


# ---------------------------------------------------------------------------
# Configuration defaults
# ---------------------------------------------------------------------------
DEFAULT_TIMEOUT = (6, 18)
DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0 Safari/537.36"
)
MAX_HTML_LENGTH = 1_800_000   # 1.8 MB safety cap per HTML response
MAX_SCRIPT_LENGTH = 1_500_000 # Max size for downloaded JS assets
SCRIPT_SCAN_LIMIT = 5         # Max number of scripts inspected per page
RATE_LIMIT_BASE = 0.35        # seconds between requests per host (jitter added)


# Candidate discovery
LIKELY_TEAM_PATHS = [
    "/team",
    "/teams",
    "/people",
    "/leadership",
    "/our-team",
    "/company/team",
    "/about",
    "/about-us",
    "/about/team",
    "/management",
    "/who-we-are",
    "/board",
    "/crew",
    "/company",
    "/partners",
    "/over-ons",
]

TEAM_ANCHOR_HINT = re.compile(
    r"\b(team|people|leadership|management|board|partners|crew|about|"
    r"ons[-\s]?team|over[-\s]?ons|wie\s+zijn\s+wij|organisatie)\b",
    re.I,
)

PEOPLE_CLASS_HINT = re.compile(
    r"(team|member|person|people|staff|leadership|management|board|bio|list-team|list-team-inner)",
    re.I,
)


# Extraction helpers
EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
DEOBFUSCATIONS = [
    (re.compile(r"\s*\[\s*at\s*\]\s*", re.I), "@"),
    (re.compile(r"\s*\(\s*at\s*\)\s*", re.I), "@"),
    (re.compile(r"\s+at\s+", re.I), "@"),
    (re.compile(r"\s*\[\s*dot\s*\]\s*", re.I), "."),
    (re.compile(r"\s*\(\s*dot\s*\)\s*", re.I), "."),
    (re.compile(r"\s+dot\s+", re.I), "."),
]

NAME_RE = re.compile(r"([A-ZÀ-ÖØ-Ý][\wÀ-ÖØ-öø-ÿ'' -]+(?: [A-ZÀ-ÖØ-Ý][\wÀ-ÖØ-öø-ÿ'' -]+){0,3})")


# Decision maker scoring
DECISION_KEYWORDS = {
    "ceo": 10,
    "chief executive": 10,
    "chief executive officer": 10,
    "founder": 8,
    "partner": 6,
    "co-founder": 7,
    "cofounder": 7,
    "chairman": 7,
    "chairwoman": 7,
    "chair": 5,
    "president": 8,
    "owner": 6,
    "managing director": 7,
    "managing partner": 7,
    "general partner": 6,
    "principal": 4,
    "chief operating officer": 7,
    "coo": 6,
    "chief financial officer": 7,
    "cfo": 6,
    "chief marketing officer": 6,
    "cmo": 5,
    "chief revenue officer": 6,
    "cro": 5,
    "chief growth officer": 5,
    "chief commercial officer": 5,
    "chief technology officer": 6,
    "cto": 5,
    "chief product officer": 5,
    "cpo": 5,
    "head of growth": 5,
    "head of sales": 5,
    "vp of sales": 4,
    "vp sales": 4,
    "vp marketing": 4,
    "head of marketing": 4,
    "head of commercial": 4,
    "board director": 4,
    "director": 10,
    "founding partner": 7,
    "oprichter": 8,
    "mede-oprichter": 7,
    "directeur": 7,
}

LINKEDIN_HINT = re.compile(r"linkedin\.com", re.I)


def keyword_in_text(text: str, keyword: str) -> bool:
    if not text or not keyword:
        return False
    k = keyword.strip()
    k_simple = re.sub(r"[\s\u00A0\u2011\u2012\u2013\u2014\u2212\-&]+", r"[\\s\\u00A0\\u2011\\u2012\\u2013\\u2014\\u2212\\-&]+", re.escape(k))

    alpha = re.sub(r"[^A-Za-zÀ-ÖØ-öø-ÿ]", "", keyword)
    if alpha:
        pattern = rf"\b{k_simple}\b"
    else:
        pattern = k_simple

    return bool(re.search(pattern, text, flags=re.I))


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------
@dataclass
class PersonCandidate:
    name: str = ""
    title: str = ""
    linkedin: str = ""
    email: str = ""
    source_url: str = ""
    score: float = 0.0
    rank_reason: str = ""

    def is_reasonable(self) -> bool:
        return bool(self.name or self.title or self.linkedin or self.email)

    def key(self) -> tuple[str, str, str, str]:
        return (
            self.name.strip().lower(),
            self.title.strip().lower(),
            self.linkedin.strip().lower(),
            self.email.strip().lower(),
        )


@dataclass
class SiteScanResult:
    website: str
    normalized_url: str = ""
    team_pages: List[str] = field(default_factory=list)
    people: List[PersonCandidate] = field(default_factory=list)
    decision_makers: List[PersonCandidate] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self, include_all_people: bool = False) -> Dict[str, Any]:
        def person_dict(person: PersonCandidate) -> Dict[str, Any]:
            data = dataclasses.asdict(person)
            return data

        payload: Dict[str, Any] = {
            "website": self.website,
            "normalized_url": self.normalized_url,
            "team_pages": self.team_pages,
            "decision_makers": [person_dict(p) for p in self.decision_makers],
        }
        if include_all_people:
            payload["people"] = [person_dict(p) for p in self.people]
        if self.errors:
            payload["errors"] = self.errors
        if self.meta:
            payload["meta"] = self.meta
        return payload


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------
def clean_email(value: str) -> str:
    if not value:
        return ""
    text = value
    for pattern, replacement in DEOBFUSCATIONS:
        text = pattern.sub(replacement, text)
    match = EMAIL_RE.search(text)
    return match.group(0) if match else ""


def normalize_url(url: str) -> str:
    if not url:
        return ""
    url = url.strip()
    if not re.match(r"^https?://", url, re.I):
        url = "https://" + url
    try:
        parts = urlsplit(url)
    except ValueError:
        return ""
    if not parts.netloc:
        return ""
    cleaned = parts._replace(fragment="", query="")
    if cleaned.path and not cleaned.path.startswith("/"):
        cleaned = cleaned._replace(path="/" + cleaned.path)
    return urlunsplit(cleaned)


def is_name_like(value: str) -> bool:
    if not value:
        return False
    value = value.strip()
    if len(value) < 2 or len(value) > 120:
        return False
    return bool(NAME_RE.fullmatch(value))


def jitter_delay(base: float) -> float:
    return base + random.random() * base


def unescape_js_string(value: str) -> str:
    if not value:
        return ""
    cleaned = value.replace("\\/", "/")
    try:
        cleaned = bytes(cleaned, "utf-8").decode("unicode_escape")
    except Exception:
        pass
    return cleaned.strip()


# ---------------------------------------------------------------------------
# Core enricher
# ---------------------------------------------------------------------------
class PeopleEnricher:
    def __init__(
        self,
        timeout: tuple[int, int] = DEFAULT_TIMEOUT,
        max_pages: int = 25,
        decision_limit: int = 5,
        include_all_people: bool = False,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.timeout = timeout
        self.max_pages = max_pages
        self.decision_limit = decision_limit
        self.include_all_people = include_all_people
        self.logger = logger or logging.getLogger("people_enricher")

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": DEFAULT_UA,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            }
        )
        self._last_request: Dict[str, float] = {}
        self._resource_cache: Dict[str, Optional[str]] = {}
        self._script_field_cache: Dict[str, re.Pattern[str]] = {}

    # ------------------------- HTTP helpers -------------------------
    def _rate_limit(self, host: str) -> None:
        now = time.time()
        last = self._last_request.get(host)
        if last is not None:
            delta = now - last
            wait = jitter_delay(RATE_LIMIT_BASE) - delta
            if wait > 0:
                time.sleep(wait)
        self._last_request[host] = time.time()

    def _fetch(self, url: str) -> Optional[requests.Response]:
        try:
            parts = urlsplit(url)
        except ValueError:
            self.logger.debug("Invalid URL skipped: %s", url)
            return None
        self._rate_limit(parts.netloc)
        try:
            resp = self.session.get(url, timeout=self.timeout, allow_redirects=True)
        except requests.RequestException as exc:
            self.logger.debug("Request error for %s: %s", url, exc)
            return None
        if not (200 <= resp.status_code < 400):
            self.logger.debug("Non-OK status %s for %s", resp.status_code, url)
            return None
        content_type = resp.headers.get("Content-Type", "")
        if "text/html" not in content_type.lower():
            self.logger.debug("Non-HTML content at %s: %s", url, content_type)
            return None
        if len(resp.content) > MAX_HTML_LENGTH:
            self.logger.debug("Response at %s too large (%d bytes)", url, len(resp.content))
            return None
        return resp

    def _fetch_text_resource(self, url: str, max_length: int = MAX_SCRIPT_LENGTH) -> Optional[str]:
        cached = self._resource_cache.get(url)
        if cached is not None:
            return cached or None

        try:
            parts = urlsplit(url)
        except ValueError:
            self.logger.debug("Invalid script URL skipped: %s", url)
            self._resource_cache[url] = ""
            return None

        self._rate_limit(parts.netloc)
        try:
            resp = self.session.get(url, timeout=self.timeout, allow_redirects=True)
        except requests.RequestException as exc:
            self.logger.debug("Script request error for %s: %s", url, exc)
            self._resource_cache[url] = ""
            return None

        if not (200 <= resp.status_code < 400):
            self.logger.debug("Script non-OK status %s for %s", resp.status_code, url)
            self._resource_cache[url] = ""
            return None

        if len(resp.content) > max_length:
            self.logger.debug("Script at %s too large (%d bytes)", url, len(resp.content))
            self._resource_cache[url] = ""
            return None

        content_type = resp.headers.get("Content-Type", "").lower()
        if not any(token in content_type for token in ("javascript", "text", "json")):
            self.logger.debug("Ignoring non-text script content at %s (%s)", url, content_type)
            self._resource_cache[url] = ""
            return None

        text = resp.text or ""
        self._resource_cache[url] = text
        return text or None

    def _find_script_field(self, chunk: str, keys: Iterable[str]) -> str:
        for key in keys:
            pattern = self._script_field_cache.get(key)
            if pattern is None:
                pattern = re.compile(
                    rf'["\']?{re.escape(key)}["\']?\s*:\s*(["\'`])(.+?)\1',
                    re.I | re.S,
                )
                self._script_field_cache[key] = pattern
            match = pattern.search(chunk)
            if match:
                value = unescape_js_string(match.group(2))
                value = re.sub(r"\s+", " ", value).strip()
                if value:
                    return value
        return ""

    def _extract_people_from_script_text(
        self,
        script_text: str,
        source_url: str,
        base_url: str,
    ) -> List[PersonCandidate]:
        if not script_text:
            return []

        people: List[PersonCandidate] = []
        name_pattern = re.compile(
            r'["\']?name["\']?\s*:\s*(["\'`])(.+?)\1',
            re.I | re.S,
        )

        for match in name_pattern.finditer(script_text):
            raw_name = unescape_js_string(match.group(2))
            name = raw_name.strip()
            if not is_name_like(name):
                continue

            brace_end = script_text.find("}", match.end())
            if brace_end == -1:
                brace_end = min(len(script_text), match.end() + 400)

            chunk = script_text[max(0, match.start() - 50) : min(len(script_text), brace_end + 1)]

            title = self._find_script_field(chunk, ("position", "title", "role"))
            linkedin_raw = self._find_script_field(chunk, ("linkedin", "linkedinUrl", "profile"))
            email_raw = self._find_script_field(chunk, ("email", "mail"))

            linkedin = self._normalize_linkedin(urljoin(base_url, linkedin_raw))
            email = clean_email(email_raw)

            if not (title or linkedin or email):
                continue

            people.append(
                PersonCandidate(
                    name=name,
                    title=title,
                    linkedin=linkedin,
                    email=email,
                    source_url=source_url,
                )
            )

        return people

    def _extract_people_from_scripts(self, soup: BeautifulSoup, base_url: str) -> List[PersonCandidate]:
        base_host = urlsplit(base_url).netloc.lower()
        script_texts: List[tuple[str, str]] = []

        for script in soup.find_all("script"):
            script_type = (script.get("type") or "").lower()
            if "ld+json" in script_type:
                continue

            src = script.get("src")
            if src:
                full_url = urljoin(base_url, src)
                if urlsplit(full_url).netloc.lower() != base_host:
                    continue
                text = self._fetch_text_resource(full_url)
                if text:
                    script_texts.append((text, full_url))
            else:
                inline = script.string or script.get_text()
                if inline:
                    snippet = inline if len(inline) <= MAX_SCRIPT_LENGTH else inline[: MAX_SCRIPT_LENGTH]
                    script_texts.append((snippet, base_url))

            if len(script_texts) >= SCRIPT_SCAN_LIMIT:
                break

        people: List[PersonCandidate] = []
        for text, origin in script_texts:
            people.extend(self._extract_people_from_script_text(text, origin, base_url))
        return people

    # --------------------- Candidate discovery ----------------------
    def discover_team_pages(self, base_url: str) -> List[str]:
        parts = urlsplit(base_url)
        root = f"{parts.scheme}://{parts.netloc}"

        candidates = [root.rstrip("/")]
        for path in LIKELY_TEAM_PATHS:
            candidates.append(urljoin(root, path))

        homepage = self._fetch(root + "/")
        if homepage:
            soup = BeautifulSoup(homepage.text, "html.parser")

            def _strip_www(h: str) -> str:
                h = (h or "").lower()
                return h[4:] if h.startswith("www.") else h

            home_netloc = _strip_www(urlsplit(homepage.url).netloc)

            for anchor in soup.find_all("a", href=True):
                href = anchor["href"].strip()
                text = anchor.get_text(" ", strip=True)

                full = urljoin(homepage.url, href)
                full_netloc = _strip_www(urlsplit(full).netloc)

                if full_netloc != home_netloc:
                    continue

                if TEAM_ANCHOR_HINT.search(href) or TEAM_ANCHOR_HINT.search(text):
                    candidates.append(full)

        cleaned: List[str] = []
        seen: set[str] = set()
        for candidate in candidates:
            candidate = candidate.split("#", 1)[0].rstrip("/")
            if candidate and candidate not in seen:
                seen.add(candidate)
                cleaned.append(candidate)
                if len(cleaned) >= self.max_pages:
                    break
        return cleaned

    # ----------------------- Extraction logic -----------------------
    def _extract_people_from_html(
        self,
        html_text: str,
        base_url: str,
        soup: Optional[BeautifulSoup] = None,
    ) -> List[PersonCandidate]:
        if soup is None:
            soup = BeautifulSoup(html_text, "html.parser")
        people: List[PersonCandidate] = []

        def add_person(name: str, title: str, linkedin: str, email: str) -> None:
            candidate = PersonCandidate(
                name=(name or "").strip(),
                title=(title or "").strip(),
                linkedin=self._normalize_linkedin(linkedin or ""),
                email=clean_email(email or ""),
                source_url=base_url,
            )
            if candidate.is_reasonable():
                people.append(candidate)

        def _extract_from_card_container(container: Tag) -> int:
            if not isinstance(container, Tag):
                return 0

            CARD_SELECTORS = [
                ".list-team-inner",
                "article", "li",
                ".team-member", ".member", ".person", ".profile", ".staff",
                ".card", ".team__item", ".grid > div", ".col", ".item",
            ]

            seen_ids: set[int] = set()
            cards: List[Tag] = []
            for sel in CARD_SELECTORS:
                for el in container.select(sel):
                    if id(el) not in seen_ids:
                        seen_ids.add(id(el))
                        cards.append(el)

            count = 0
            for card in cards:
                text = card.get_text("\n", strip=True)
                lines = [s.strip() for s in text.split("\n") if s.strip()]

                name = ""
                for sel in [
                    "h1","h2","h3","h4","strong","b",
                    ".name",".person-name",".member-name",".team__name",".profile-name",
                ]:
                    t = card.select_one(sel)
                    if t:
                        cand = t.get_text(" ", strip=True)
                        if cand and len(cand) <= 120:
                            name = cand
                            break
                if not name and lines:
                    name = lines[0]

                title = ""
                if len(lines) >= 2:
                    title = lines[1]
                if not title:
                    ttag = card.find(["em","small","span","p"])
                    if ttag:
                        cand = ttag.get_text(" ", strip=True)
                        if 3 <= len(cand) <= 120:
                            title = cand

                li = card.select_one('a[href*="linkedin.com"]')
                linkedin = urljoin(base_url, li.get("href", "")) if li else ""
                email_match = re.search(
                    r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}",
                    card.get_text(" ", strip=True),
                    re.I,
                )
                email = email_match.group(0) if email_match else ""

                if name or title or linkedin or email:
                    add_person(name, title, linkedin, email)
                    count += 1

            return count

        # 1) Blocks with class/id hints
        for block in soup.find_all(True):
            if not isinstance(block, Tag):
                continue
            class_id_blob = " ".join(
                [" ".join(block.get("class", []) or []), block.get("id") or "", block.name or ""]
            )
            if not PEOPLE_CLASS_HINT.search(class_id_blob):
                continue

            if _extract_from_card_container(block) > 0:
                continue

            text = re.sub(r"\s+", " ", " ".join(block.stripped_strings))
            if not text:
                continue
            name = self._extract_name_from_block(block, text)
            title = self._extract_title_from_block(block, text, name)
            linkedin = self._extract_linkedin(block, base_url)
            email = self._extract_email(block, text)
            add_person(name, title, linkedin, email)

        # 2) JSON-LD Person
        for node in soup.find_all("script", type=lambda t: t and "ld+json" in t.lower()):
            script_text = node.string or node.get_text()
            if not script_text:
                continue
            try:
                data = json.loads(script_text)
            except json.JSONDecodeError:
                data = None

            def walk(v: Any) -> None:
                if isinstance(v, dict):
                    if (v.get("@type") or "").lower() == "person":
                        name = str(v.get("name") or "").strip()
                        title = str(v.get("jobTitle") or "").strip()
                        email = str(v.get("email") or "").strip()
                        same_as = v.get("sameAs") or []
                        linkedin = ""
                        if isinstance(same_as, str):
                            same_as = [same_as]
                        for entry in same_as:
                            linkedin = self._normalize_linkedin(str(entry))
                            if linkedin:
                                break
                        add_person(name, title, linkedin, email)
                    for nv in v.values():
                        walk(nv)
                elif isinstance(v, list):
                    for it in v:
                        walk(it)

            if data is not None:
                walk(data)

        # 3) Microdata Person
        for tag in soup.find_all(attrs={"itemtype": re.compile("Person", re.I)}):
            name = ""
            if tag.get("itemprop") == "name":
                name = tag.get_text(" ", strip=True)
            else:
                tname = tag.find(attrs={"itemprop": re.compile("^name$", re.I)})
                if tname:
                    name = tname.get_text(" ", strip=True)
            ttitle = tag.find(attrs={"itemprop": re.compile("jobTitle", re.I)})
            title = ttitle.get_text(" ", strip=True) if ttitle else ""
            linkedin = ""
            for a in tag.find_all("a", href=True):
                linkedin = self._normalize_linkedin(urljoin(base_url, a["href"]))
                if linkedin:
                    break
            email = ""
            mail = tag.find("a", href=re.compile("^mailto:", re.I))
            if mail and mail.get("href"):
                email = clean_email(mail["href"][7:])
            if not email:
                email = clean_email(tag.get_text(" ", strip=True))
            add_person(name, title, linkedin, email)

        # 4) Generic anchors: emails + LinkedIn
        for anchor in soup.find_all("a", href=True):
            href = anchor["href"]
            if href.lower().startswith("mailto:"):
                name = anchor.get_text(" ", strip=True)
                email = clean_email(href[7:])
                if email:
                    add_person(name, "", "", email)
                continue
            if LINKEDIN_HINT.search(href):
                blk = anchor
                for _ in range(3):
                    p = blk.parent
                    if not p:
                        break
                    txt = p.get_text(" ", strip=True) if hasattr(p, "get_text") else ""
                    if txt and len(txt) <= 2000:
                        blk = p
                    else:
                        break
                name = anchor.get_text(" ", strip=True)
                if not is_name_like(name):
                    for sel in ["h1","h2","h3","h4","strong","b",
                                ".name",".person-name",".member-name",".team__name",".profile-name"]:
                        t = blk.select_one(sel)
                        if t:
                            cand = t.get_text(" ", strip=True)
                            if is_name_like(cand):
                                name = cand
                                break
                title = ""
                block_text = blk.get_text(" ", strip=True)
                if block_text and name and name in block_text:
                    after = block_text.split(name, 1)[1].strip()
                    m = re.search(r"([A-Z][A-Za-zÀ-ÖØ-öø-ÿ0-9&.,'' \-/]{3,100})", after)
                    if m:
                        title = m.group(1)
                if not title:
                    ttag = blk.find(["em","small","span","p"])
                    if ttag:
                        cand = ttag.get_text(" ", strip=True)
                        if 3 <= len(cand) <= 120:
                            title = cand
                add_person(name, title, urljoin(base_url, href), "")

        # 5) Headings near role keywords
        for heading in soup.find_all(["h1", "h2", "h3", "h4"]):
            heading_text = heading.get_text(" ", strip=True)
            block_text = heading_text
            sibling = heading.find_next_sibling()
            if sibling:
                block_text += " " + sibling.get_text(" ", strip=True)
            block_text = re.sub(r"\s+", " ", block_text)
            if not block_text:
                continue
            lower = block_text.lower()
            if any(keyword_in_text(lower, k) for k in DECISION_KEYWORDS.keys()):
                m = NAME_RE.search(block_text)
                name = m.group(1) if m else ""
                linkedin = ""
                li = heading.find("a", href=LINKEDIN_HINT) or (sibling and sibling.find("a", href=LINKEDIN_HINT))
                if li:
                    linkedin = urljoin(base_url, li.get("href", ""))
                email = clean_email(block_text)
                add_person(name, heading_text, linkedin, email)

        # 6) JS bundles
        people.extend(self._extract_people_from_scripts(soup, base_url))

        # Dedup
        unique: Dict[tuple[str, str, str, str], PersonCandidate] = {}
        for p in people:
            key = p.key()
            if key in unique:
                e = unique[key]
                if not e.source_url and p.source_url:
                    e.source_url = p.source_url
                if not e.title and p.title:
                    e.title = p.title
                if not e.linkedin and p.linkedin:
                    e.linkedin = p.linkedin
                if not e.email and p.email:
                    e.email = p.email
            else:
                unique[key] = p
        return list(unique.values())

    def _extract_name_from_block(self, block: Tag, text: str) -> str:
        selectors = [
            "strong",
            "b",
            "h1",
            "h2",
            "h3",
            "h4",
            ".name",
            ".team__name",
            ".member-name",
            ".person-name",
            ".profile-name",
        ]
        for selector in selectors:
            element = block.select_one(selector)
            if element:
                candidate = element.get_text(" ", strip=True)
                if is_name_like(candidate):
                    return candidate
        match = NAME_RE.search(text)
        return match.group(1) if match and is_name_like(match.group(1)) else ""

    def _extract_title_from_block(self, block: Tag, text: str, name: str) -> str:
        name = name or ""
        title = ""
        if name and name in text:
            after_name = text[text.find(name) + len(name) :]
            match = re.search(r"([A-Z][A-Za-z0-9&.,'' \-/]{3,100})", after_name.strip())
            if match:
                title = match.group(1)
        if not title:
            fallback = block.find(["em", "small", "p", "span"])
            if fallback:
                candidate = fallback.get_text(" ", strip=True)
                if 3 <= len(candidate) <= 120:
                    title = candidate
        return title

    def _extract_linkedin(self, block: Tag, base_url: str) -> str:
        for anchor in block.find_all("a", href=True):
            href = anchor["href"]
            if LINKEDIN_HINT.search(href):
                return self._normalize_linkedin(urljoin(base_url, href))
        return ""

    def _extract_email(self, block: Tag, text_blob: str) -> str:
        for anchor in block.find_all("a", href=True):
            href = anchor["href"]
            if href.lower().startswith("mailto:"):
                return clean_email(href[7:])
        return clean_email(text_blob)

    def _normalize_linkedin(self, url: str) -> str:
        if not url:
            return ""
        url = url.strip()
        if url.startswith("//"):
            url = "https:" + url
        try:
            parts = urlsplit(url)
        except ValueError:
            return ""
        if "linkedin.com" not in parts.netloc.lower():
            return ""
        path = re.sub(r"/+$", "", parts.path or "")
        if not path.startswith("/"):
            path = "/" + path
        normalized = urlunsplit(("https", "www.linkedin.com", path, "", ""))
        return normalized

    # -------------------- Scoring & selection -----------------------
    def _score_person(self, person):
        score = 0.0
        title_lower = person.title.lower()
        name_lower = person.name.lower()

        for keyword, weight in DECISION_KEYWORDS.items():
            if keyword_in_text(title_lower, keyword):
                score += weight

        if "chief" in title_lower:
            score += 2
        if keyword_in_text(title_lower, "vp") and "vice" not in title_lower:
            score += 1
        if person.linkedin:
            score += 1.5
        if person.email:
            score += 1.5

        try:
            path = urlsplit(person.source_url).path.lower()
            if any(k in path for k in ("team", "people", "about", "over-ons", "leadership", "management")):
                score += 0.5
        except Exception:
            pass

        reason_parts = []
        if person.email:
            reason_parts.append("email")
        if person.linkedin:
            reason_parts.append("linkedin")
        keywords_hit = [k for k in DECISION_KEYWORDS if keyword_in_text(title_lower, k)]
        if keywords_hit:
            reason_parts.extend(keywords_hit[:3])

        person.score = score
        person.rank_reason = ", ".join(reason_parts)
        return person

    def _select_decision_makers(self, people):
        scored = [self._score_person(dataclasses.replace(p)) for p in people]
        scored.sort(key=lambda p: p.score, reverse=True)

        selected = []
        for person in scored:
            if len(selected) >= self.decision_limit:
                break
            if person.score >= 3.0 or (not selected and person.score >= 1.5):
                selected.append(person)

        if not selected and scored:
            fallback = scored[0]
            fallback.rank_reason = (fallback.rank_reason + ", fallback").strip(", ")
            selected.append(fallback)
        return selected

    # ------------------------- Public API ---------------------------
    def scan_site(self, website: str) -> SiteScanResult:
        result = SiteScanResult(website=website)
        normalized = normalize_url(website)
        if not normalized:
            result.errors.append("invalid_url")
            return result
        result.normalized_url = normalized

        candidate_pages = self.discover_team_pages(normalized)

        self.logger.debug("CANDIDATE PAGES for %s: %s", normalized, candidate_pages)

        if not candidate_pages:
            result.errors.append("no_candidate_pages")
            return result
        result.team_pages = candidate_pages

        all_people: List[PersonCandidate] = []
        searched_pages = 0

        for page_url in candidate_pages:
            if searched_pages >= self.max_pages:
                break
            searched_pages += 1
            response = self._fetch(page_url)
            if not response:
                continue
            extracted = self._extract_people_from_html(response.text, response.url)
            if extracted:
                all_people.extend(extracted)

        if not all_people:
            result.errors.append("no_people_found")
            return result

        deduped: Dict[tuple[str, str, str, str], PersonCandidate] = {}
        for person in all_people:
            key = person.key()
            if key not in deduped:
                deduped[key] = person
            else:
                existing = deduped[key]
                if not existing.source_url and person.source_url:
                    existing.source_url = person.source_url
                if not existing.title and person.title:
                    existing.title = person.title
                if not existing.linkedin and person.linkedin:
                    existing.linkedin = person.linkedin
                if not existing.email and person.email:
                    existing.email = person.email

        unique_people = list(deduped.values())
        result.people = [self._score_person(person) for person in unique_people]
        result.people.sort(key=lambda p: p.score, reverse=True)
        result.decision_makers = self._select_decision_makers(unique_people)
        result.meta["people_examined"] = len(unique_people)
        result.meta["pages_scanned"] = searched_pages
        return result


# ---------------------------------------------------------------------------
# Input helpers
# ---------------------------------------------------------------------------
def load_websites(files: Iterable[str], inline_urls: Iterable[str]) -> List[str]:
    websites: List[str] = []

    def add_url(value: str) -> None:
        cleaned = (value or "").strip()
        if cleaned:
            websites.append(cleaned)

    for url in inline_urls:
        add_url(url)

    for path in files:
        path = path.strip()
        if not path:
            continue
        try:
            with open(path, "r", encoding="utf-8") as handle:
                raw = handle.read()
        except OSError as exc:
            logging.warning("Failed to read %s: %s", path, exc)
            continue

        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = None

        if isinstance(parsed, list):
            for entry in parsed:
                if isinstance(entry, str):
                    add_url(entry)
                elif isinstance(entry, dict):
                    add_url(entry.get("website") or entry.get("url") or "")
            continue
        if isinstance(parsed, dict):
            items = parsed.get("websites") or parsed.get("urls")
            if isinstance(items, list):
                for entry in items:
                    if isinstance(entry, str):
                        add_url(entry)
            continue

        for line in raw.splitlines():
            add_url(line)

    if not websites and not sys.stdin.isatty():
        for line in sys.stdin:
            add_url(line)

    seen: set[str] = set()
    ordered: List[str] = []
    for url in websites:
        if url not in seen:
            seen.add(url)
            ordered.append(url)
    return ordered


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Discover decision makers from company websites."
    )
    parser.add_argument(
        "--input-file",
        "-i",
        action="append",
        default=[],
        help="File containing websites (JSON list or newline separated). Can be used multiple times.",
    )
    parser.add_argument(
        "--url",
        "-u",
        action="append",
        default=[],
        help="Individual website URL. Can be supplied multiple times.",
    )
    parser.add_argument(
        "--output",
        "-o",
        default="enriched_people.json",
        help="Path to the output JSON file.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=25,
        help="Maximum candidate pages to scan per site.",
    )
    parser.add_argument(
        "--decision-limit",
        type=int,
        default=5,
        help="Maximum number of decision makers to include per site.",
    )
    parser.add_argument(
        "--include-all-people",
        action="store_true",
        help="Include all discovered people in the output (not just decision makers).",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="count",
        default=0,
        help="Increase logging verbosity (use -vv for debug).",
    )
    return parser.parse_args(argv)


def configure_logging(verbosity: int) -> None:
    if verbosity >= 2:
        level = logging.DEBUG
    elif verbosity == 1:
        level = logging.INFO
    else:
        level = logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    configure_logging(args.verbose)

    websites = load_websites(args.input_file, args.url)
    if not websites:
        logging.error("No websites provided. Use --url or --input-file, or pipe URLs via stdin.")
        return 1

    enricher = PeopleEnricher(
        max_pages=args.max_pages,
        decision_limit=args.decision_limit,
        include_all_people=args.include_all_people,
        logger=logging.getLogger("people_enricher"),
    )

    results: List[Dict[str, Any]] = []
    for website in websites:
        logging.info("Scanning %s", website)
        result = enricher.scan_site(website)
        results.append(result.to_dict(include_all_people=args.include_all_people))

    try:
        with open(args.output, "w", encoding="utf-8") as handle:
            json.dump(results, handle, ensure_ascii=False, indent=2)
    except OSError as exc:
        logging.error("Failed to write %s: %s", args.output, exc)
        return 2

    logging.info("Wrote %d site results to %s", len(results), args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main())