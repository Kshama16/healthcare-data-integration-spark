"""Downloads the current CMS Provider Data files into data/raw/.

Looks up each dataset's current CSV in the CMS metastore, so the pipeline always
pulls the latest published release.
"""
import json
import ssl
import urllib.request
from pathlib import Path

import certifi

# python.org builds of Python on macOS ship without root certificates; use certifi's bundle.
SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())

RAW = Path(__file__).resolve().parents[1] / "data" / "raw"

DATASETS = {
    "xubh-q36u": "hospital_general_information.csv",
    "ynj2-r877": "complications_and_deaths.csv",
    "632h-zaca": "unplanned_hospital_visits.csv",
    "77hc-ibv8": "healthcare_associated_infections.csv",
    "dgck-syfz": "hcahps_patient_survey.csv",
    "yv7e-xc69": "timely_and_effective_care.csv",
}
METASTORE = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/{}"


def main():
    RAW.mkdir(parents=True, exist_ok=True)
    for dataset_id, fname in DATASETS.items():
        with urllib.request.urlopen(METASTORE.format(dataset_id), timeout=60, context=SSL_CONTEXT) as resp:
            meta = json.load(resp)
        url = meta["distribution"][0]["downloadURL"]
        print(f"{meta['title']} (modified {meta['modified']})")
        with urllib.request.urlopen(url, timeout=300, context=SSL_CONTEXT) as resp:
            (RAW / fname).write_bytes(resp.read())


if __name__ == "__main__":
    main()
