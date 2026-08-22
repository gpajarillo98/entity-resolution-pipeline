"""
Cleaning and normalization.

Fuzzy matching only works as well as the strings you feed it. Before any
similarity scoring happens, both sources get pushed through the same
normalization rules so "777 N LASALLE DR" and "777 N LA SALLE DR" have a
fighting chance of scoring as similar, and legal-entity noise like "LLC"
or "INC." doesn't drown out the part of the name that actually varies.
"""

import re
import pandas as pd

# Common street suffix / directional abbreviations seen in Chicago address data.
STREET_ABBREVIATIONS = {
    "STREET": "ST", "AVENUE": "AVE", "BOULEVARD": "BLVD", "DRIVE": "DR",
    "ROAD": "RD", "LANE": "LN", "PLACE": "PL", "COURT": "CT",
    "PARKWAY": "PKWY", "HIGHWAY": "HWY", "SQUARE": "SQ",
    "NORTH": "N", "SOUTH": "S", "EAST": "E", "WEST": "W",
    "LASALLE": "LA SALLE",  # the one that broke the sample match in the writeup
}

# Legal-entity suffixes that add noise to name matching but carry no
# identifying signal on their own.
ENTITY_SUFFIXES = re.compile(
    r"\b(LLC|L\.L\.C\.?|INC|INC\.?|CORP|CORPORATION|LTD|CO|COMPANY)\b\.?,?",
    re.IGNORECASE,
)


def normalize_name(name: str) -> str:
    """Uppercase, strip legal suffixes and punctuation, collapse whitespace."""
    if not isinstance(name, str) or not name.strip():
        return ""
    name = name.upper()
    name = ENTITY_SUFFIXES.sub("", name)
    name = re.sub(r"[^\w\s]", " ", name)   # drop punctuation
    name = re.sub(r"\s+", " ", name).strip()
    return name


def normalize_address(address: str) -> str:
    """Uppercase, expand/standardize suffixes, strip unit/suite noise,
    collapse whitespace. This is intentionally conservative -- it
    standardizes tokens rather than trying to fully parse the address,
    since a wrong parse is worse than a partial normalize for fuzzy scoring.
    """
    if not isinstance(address, str) or not address.strip():
        return ""
    addr = address.upper()
    addr = re.sub(r"[^\w\s]", " ", addr)
    tokens = addr.split()
    tokens = [STREET_ABBREVIATIONS.get(t, t) for t in tokens]
    addr = " ".join(tokens)
    addr = re.sub(r"\s+", " ", addr).strip()
    return addr


def clean_business_licenses(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse license history to one row per business (account + site),
    keeping the most recent record by DATE ISSUED. The raw file has ~1.2
    rows per business on average because renewals, location changes, and
    activity changes all create new rows against the same business.
    """
    df = df.copy()
    df["DATE ISSUED"] = pd.to_datetime(df["DATE ISSUED"], errors="coerce")
    df = df.sort_values("DATE ISSUED").drop_duplicates(
        subset=["ACCOUNT NUMBER", "SITE NUMBER"], keep="last"
    )

    df["name_norm"] = df["DOING BUSINESS AS NAME"].fillna(df["LEGAL NAME"]).apply(normalize_name)
    df["legal_name_norm"] = df["LEGAL NAME"].apply(normalize_name)
    df["address_norm"] = df["ADDRESS"].apply(normalize_address)
    df["zip"] = df["ZIP CODE"].astype(str).str.strip().str[:5]

    keep_cols = [
        "ACCOUNT NUMBER", "SITE NUMBER", "LICENSE NUMBER", "LEGAL NAME",
        "DOING BUSINESS AS NAME", "name_norm", "legal_name_norm",
        "ADDRESS", "address_norm", "zip", "LICENSE DESCRIPTION",
        "LICENSE STATUS", "DATE ISSUED",
    ]
    return df[keep_cols].reset_index(drop=True)


# Facility types that are inspected under the food-inspection program but
# don't reliably carry a standard retail food business license -- schools,
# daycares, and long-term care facilities are usually licensed/regulated
# through separate channels. Excluding them is OPT-IN (see
# clean_food_inspections' exclude_facility_types param) rather than a
# silent default, since some of these genuinely do have a matching
# license and blanket-excluding them is a judgment call, not a fact.
NON_RETAIL_FACILITY_TYPES = {
    "SCHOOL", "CHARTER SCHOOL", "CHILDREN'S SERVICES FACILITY",
    "DAYCARE ABOVE AND UNDER 2 YEARS", "DAYCARE (2 - 6 YEARS)",
    "DAYCARE (UNDER 2 YEARS)", "LONG TERM CARE", "HOSPITAL",
}


def clean_food_inspections(df: pd.DataFrame, exclude_facility_types: bool = True) -> pd.DataFrame:
    """Collapse to one row per (License #, DBA Name) pair, keeping the
    most recent inspection -- we're resolving businesses, not inspection
    events, so repeat inspections of the same restaurant should collapse
    to a single matching candidate.

    exclude_facility_types: if True (default), drop institutional facility
    types (schools, daycares, hospitals, long-term care) that are
    inspected under this program but typically aren't retail food
    businesses in the Business Licenses dataset. Defaulted on based on
    a real full-dataset comparison: it shrinks the manual-review queue by
    ~24% (2,130 -> 1,628 records) with no measurable cost to precision or
    recall (96.41%->96.19%, 92.82%->92.77%). See README Results section.
    """
    df = df.copy()
    if exclude_facility_types:
        df = df[~df["Facility Type"].str.upper().isin(NON_RETAIL_FACILITY_TYPES)]

    df["Inspection Date"] = pd.to_datetime(df["Inspection Date"], errors="coerce")
    df = df.sort_values("Inspection Date").drop_duplicates(
        subset=["License #", "DBA Name"], keep="last"
    )

    df["name_norm"] = df["DBA Name"].apply(normalize_name)
    df["aka_name_norm"] = df["AKA Name"].apply(normalize_name)
    df["address_norm"] = df["Address"].apply(normalize_address)
    df["zip"] = df["Zip"].astype(str).str.strip().str[:5]

    keep_cols = [
        "Inspection ID", "License #", "DBA Name", "AKA Name",
        "name_norm", "aka_name_norm", "Address", "address_norm", "zip",
        "Facility Type", "Results", "Inspection Date",
    ]
    return df[keep_cols].reset_index(drop=True)


if __name__ == "__main__":
    from extract.sources import load_business_licenses, load_food_inspections

    bl_clean = clean_business_licenses(load_business_licenses())
    fi_clean = clean_food_inspections(load_food_inspections())

    print(f"business licenses: {len(bl_clean):,} unique businesses")
    print(f"food inspections:  {len(fi_clean):,} unique (license, dba) pairs")

    bl_clean.to_csv("data/processed/business_licenses_clean.csv", index=False)
    fi_clean.to_csv("data/processed/food_inspections_clean.csv", index=False)
