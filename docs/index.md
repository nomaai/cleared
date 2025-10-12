# Cleared

<div align="center">
  <img src="logo.png" alt="Cleared Logo" width="200">
</div>

> Share data for scientific research confidently.

---

## 🩺 Overview

**Cleared** is an opensource multi-purpose de-identification library with special support for the healthcare applications. It provides robust tools to de-identify **multi-table, multimodal** datasets while maintaining clinical integrity and research utility. Therefore, it provides support for wellknown healthcare usecases in addition to predefined controls and configurations for typical  compliance levels optimized for **HIPAA**, **GDPR**, and **Safe Harbor** standards. Examples include:

- Support for multiple identifiers (SSN, Encounter Id, MRN, FIN, etc) in the same tables
- Time-field de-identification
- Patient-aware deidentification across multiple encounters (visits)
- Date and time de-identification both at column-level and row value level.
- Support for time-series data such as multi-variate sparsely sampled data types and high-frequencyt waveforms
- Predefined configurations for standard schemas such as [OMOP CDM](https://www.ohdsi.org/data-standardization/).

---

## 🔬 Healthcare Use Cases

| Use Case | Description |
|-----------|-------------|
| **EHR De-identification** | De-identify patient, encounter, and observation tables while preserving linkages |
| **Clinical Research Prep** | Create HIPAA-compliant datasets for multicenter research or data sharing |
| **Machine Learning Pipelines** | Prepare structured and text data for model training without PHI |
| **Temporal Cohort Studies** | Shift and anonymize dates while preserving relative event order |
| **FHIR/OMOP Data Conversion** | Ingest and export data to common healthcare models safely |

---
## 🧩 Features

| Feature | Description |
|----------|-------------|
| ✅ **Multi-table Support** | Consistent ID mapping across EHR tables (e.g. patients, encounters, labs) |
| ✅ **Multi-ID Support** | Consistent ID mapping across multiple identifiers |
| ✅ **Data Risk Analysis and Reporting** | Analyzes datasets for possible identfier risk and providers comprehensive report to verify de-id plans and configurations|
| ✅ **ID Grouping Support** | Supports de-identification of group-level identifiers such as Patient/Person ID or MRN that will be common across multiple unique patient visits or encounters|
| ✅ **Date & Time Shifting** | De-identify temporal data while preserving clinical event intervals |
| ✅ **Schema-aware Configs** | Built-in support for HL7, OMOP, and FHIR-like schemas |
| ✅ **Concept ID Filtering** | Create deidentification rules in values based on concept_id filters |
| ✅ **Conditional De-identification** |  Ability to only apply de-identification rules|
| ✅ **Pseudonymization Engine** | Deterministic, reversible pseudonyms for longitudinal tracking |
| ✅ **Audit Logging** | Track all transformations for compliance and reproducibility especially as may be needed for medical devices |
| ✅ **Custom Transformers PLugins** | Supports implementation of plugins for custom de-identification filters and methods  |
| ✅ **Healthcare-Ready Defaults** | Includes mappings for demographics, identifiers, and care events |
| ✅ **Configuration Reusability** | Leverages the well-known hydra configuration yaml file to facilitate reusability of existing configs, partial configuration imoporting, configuration inheritencfe and customizations |

## ⚖️ Compliance

**Cleared** is designed to assist with developing de-identification pipelines to reach compliance under the following frameworks and standards:

- **HIPAA** (Safe Harbor & Expert Determination)
- **GDPR** (Anonymization & Pseudonymization)
- **21 CFR Part 11** (Audit Trails)

> ⚠️ **Note:** Cleared is a toolkit — not a certification engine.  
> Regulatory compliance remains **user-dependent** and must be validated within your organization’s governance and compliance framework.



## 📚 Getting Started

1. [Quickstart](quickstart.md)
2. [Multi-table De-identification](multi-table-deidentification.md)
3. [Group-level ID de-identification](group-level-id-deidentification.md)
4. [Date and Time Shifting](date-and-time-shifting.md)
5. [Free-text PHI Detection](phi-detection.md)
6. [Logging and Auditing](logging-and-auditing.md)
7. [Healthcare Schema Support](healthcare-schema-support.md)
8. [Custom Transformers Plugins](custom-transformers-plugins.md)
10. [Contributing](contributing.md)

## 🛣 Roadmap

| Milestone                                    | Status       |
|---------------------------------------------|--------------|
| Multi-table, Multi-id de-ID                  | ✅ Completed |
| Concept based filtering                      | ✅ Completed |
| Standard PHI type detectors                  | ✅ Completed |
| OMOP  schema defaults                        | ✅ Completed |
| Date/time & age shifting                     | ✅ Completed |
| LLM PHI scanner                              | ⏳ Planned   |
| Synthetic patient generator                  | ⏳ Planned   |
| Integration with MIMIC-IV & PhysioNet        | ⏳ Planned   |
| Support for waveform & image metadata        | ⏳ Planned   |
| Cloud-native deployment (GCP/AWS)            | ⏳ Planned   |

---

## 🤝 Contributing

We welcome contributions from healthcare AI developers, informaticians, and data engineers.

Please see [`CONTRIBUTING.md`](CONTRIBUTING.md) for contribution guidelines.

Areas you can help with:
- ⏳ Contribute to the planned features
- 🧩 Writing new transformers
- ⛁ Implementing storage type support for Postgres/MySQL/Iceberg/etc.
- 🧰 Adding new schema built-in supports for EPIC/Cerner/etc.
- 🤖 Integrating model-based PHI detectors
- 🧪 Improving testing infrastructure and synthetic data coverage

---

## 📜 License and Disclaimer

This project is licensed under the [Apache 2.0 License](https://www.apache.org/licenses/LICENSE-2.0).

> ⚠️ Disclaimer: This library is provided "as is" without warranty of any kind. It is not a certified compliance tool. You are responsible for validating its use in regulated or clinical environments. 

**Read detailed disclaimers [here](disclaime.md)**



---

## 🌐 Links

- [📖 Documentation](https://cleared.readthedocs.io)
- [📦 PyPI Package](https://pypi.org/project/cleared)
- [📊 Demo Notebooks](https://github.com/nomaai/cleared/examples)
- [💬 Issues & Discussions](https://github.com/YOURORG/cleared/issues)

