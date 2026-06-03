"""
features/base.py — per-category feature schema primitives.

A FeatureSchema is the API-agnostic definition of *what* to extract for one
catalyst category: the typed field list, the enums, and the system prompt /
JSON-schema rendered from them. It knows nothing about batching, caching, or
the Anthropic client — features/runner.py consumes it.

Add a new category by writing a sibling module under features/schemas/
(e.g. crypto_treasury.py) that builds a FeatureSchema and calls register();
nothing here changes.

Null discipline is baked into the rendering: every field is `required` (must be
present in the output) but every type is unioned with "null", so "not stated in
the release" is expressed as null rather than a missing key. The system prompt
forbids inferring/guessing values — a wrong number is worse than null.
"""
from dataclasses import dataclass, field
from typing import Optional

# dtype -> JSON-schema base type. Everything is rendered nullable.
_JSON_BASE = {
    "number": "number",
    "integer": "integer",
    "string": "string",
    "boolean": "boolean",
    "enum": "string",   # enum values are strings; allowed set carried in `enum`
    "date": "string",   # ISO date as a plain string (no format constraint)
}


@dataclass
class FieldSpec:
    """One extracted feature -> one output column (namespaced by schema.prefix)."""
    name: str                       # column name without prefix, e.g. "gross_proceeds_m"
    dtype: str                      # number | integer | string | boolean | enum | date
    desc: str                       # extraction instruction shown to the model
    enum: Optional[list] = None     # allowed values when dtype == "enum"
    applies_when: Optional[str] = None  # note for conditional fields (security_type, ...)

    def json_property(self) -> dict:
        # Nullable is expressed with an anyOf null branch, NOT a ["type","null"]
        # union: the structured-output validator rejects an `enum` under a type
        # union ("enum value does not match declared type ['string','null']").
        base = _JSON_BASE[self.dtype]
        if self.dtype == "enum":
            if not self.enum:
                raise ValueError(f"enum field {self.name!r} has no allowed values")
            value = {"type": base, "enum": list(self.enum)}
        else:
            value = {"type": base}
        return {"anyOf": [value, {"type": "null"}], "description": self._full_desc()}

    def _full_desc(self) -> str:
        d = self.desc
        if self.applies_when:
            d += f" Applies only when {self.applies_when}; otherwise null."
        return d

    def prompt_line(self) -> str:
        if self.dtype == "enum":
            t = "enum: " + " | ".join(self.enum)
        else:
            t = self.dtype
        return f"- {self.name} ({t}): {self._full_desc()}"


@dataclass
class FeatureSchema:
    category: str           # the catalyst tag this applies to, e.g. "private_placement"
    prefix: str             # column namespace, e.g. "pp" -> pp_gross_proceeds_m
    version: str            # schema version, recorded per row for provenance
    intro: str              # one-line description of the event type, for the prompt
    fields: list            # list[FieldSpec]
    examples: list = field(default_factory=list)  # optional (body, dict) few-shot pairs
    deriver: Optional[object] = None  # optional callable(df, schema) -> DataFrame of f_ cols

    # --- column names -------------------------------------------------------
    def column_names(self) -> list:
        return [f"{self.prefix}_{f.name}" for f in self.fields]

    def namespaced(self, raw: dict) -> dict:
        """Map a model output dict {field: value} -> {prefix_field: value}, with
        every declared field present (missing -> None)."""
        return {f"{self.prefix}_{f.name}": raw.get(f.name) for f in self.fields}

    # --- API payloads -------------------------------------------------------
    def json_schema(self) -> dict:
        """JSON schema for output_config.format. All fields required (present),
        each nullable; additionalProperties:false for strict structured output."""
        return {
            "type": "object",
            "additionalProperties": False,
            "properties": {f.name: f.json_property() for f in self.fields},
            "required": [f.name for f in self.fields],
        }

    def system_prompt(self) -> str:
        lines = [
            f"You are a financial press-release feature extractor. You are given "
            f"the body of a single press release describing a {self.intro}.",
            "",
            "Extract the fields listed below into the structured output schema.",
            "",
            "Rules:",
            "- Use ONLY facts explicitly stated in THIS press release.",
            "- If a field is not stated, return null. NEVER infer, estimate, "
            "derive, or guess — especially numbers (dollar amounts, share counts, "
            "percentages, dates). A null is always better than a wrong value.",
            "- Money amounts: return the numeric magnitude in MILLIONS, in the "
            "currency given by the `currency` field "
            "(e.g. $4,800,000 -> 4.8; $350 million -> 350).",
            "- Percentages: return a plain number, not a fraction (20% -> 20).",
            "- Dates: ISO format YYYY-MM-DD.",
            "- Enum fields: choose exactly one allowed value; if none fit or it is "
            "not stated, return null.",
            "",
            "Fields:",
        ]
        lines += [f.prompt_line() for f in self.fields]
        lines += [
            "",
            "Return ONLY a single JSON object — no prose, no markdown, no code "
            "fences. It must contain EXACTLY these keys, every key present, using "
            "null when the value is not stated:",
            ", ".join(f.name for f in self.fields),
        ]
        return "\n".join(lines)


# ── registry ────────────────────────────────────────────────────────────────
REGISTRY: dict = {}


def register(schema: FeatureSchema) -> FeatureSchema:
    REGISTRY[schema.category] = schema
    return schema


def get_schema(category: str) -> FeatureSchema:
    if category not in REGISTRY:
        raise KeyError(
            f"no feature schema registered for {category!r}; "
            f"available: {sorted(REGISTRY)}"
        )
    return REGISTRY[category]
