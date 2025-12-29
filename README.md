# Cleared

<div align="center">

<img src="https://github.com/nomaai/cleared/blob/main/img/logo.png?raw=true" alt="Cleared Logo" width="200">

</div>

> Share data for scientific research confidently.

---

## 🩺 Overview

**Cleared** is an open-source multi-purpose de-identification library with special support for healthcare applications. It provides robust tools to de-identify **multi-table, multimodal** datasets while maintaining clinical integrity and research utility.


- Support for multiple identifiers (SSN, Encounter Id, MRN, FIN, etc) in the same tables
- Time-field de-identification
- Patient-aware deidentification across multiple encounters (visits)
- Date and time de-identification both at column-level and row value level.
- Support for time-series data such as multi-variate sparsely sampled data types and high-frequencyt waveforms
- Predefined configurations for standard schemas such as [OMOP CDM](https://www.ohdsi.org/data-standardization/).


<div align="center">

<img src="https://github.com/nomaai/cleared/blob/main/img/cleared-overview.png?raw=true" alt="Cleared Overview" width="100%">

</div>


---
## 🧩 Features

| Feature | Description |
|----------|-------------|
| ✅ **Multi-table Support** | Consistent ID mapping across EHR tables (e.g. patients, encounters, labs) |
| ✅ **Multi-ID Support** | Consistent ID mapping across multiple identifiers |
| ✅ **Multi-Segment Tables** | Automatic detection and processing of tables split across multiple segment files |
| ⏳ **Data Risk Analysis and Reporting** | Analyzes datasets for possible identfier risk and providers comprehensive report to verify de-id plans and configurations|
| ✅ **ID Grouping Support** | Supports de-identification of group-level identifiers such as Patient/Person ID or MRN that will be common across multiple unique patient visits or encounters|
| ✅ **Date & Time Shifting** | De-identify temporal data while preserving clinical event intervals |
| ✅ **Schema-aware Configs** | Built-in support for HL7, OMOP, and FHIR-like schemas |
| ✅ **Concept ID Filtering** | Create deidentification rules in values based on concept_id filters |
| ✅ **Conditional De-identification** |  Ability to only apply de-identification rules|
| ✅ **Pseudonymization Engine** | Deterministic, reversible pseudonyms for longitudinal tracking |
| ✅ **Reverse De-identification** | Restore original values from de-identified data using reference mappings |
| ✅ **Verify De-identification** | Verify that reversed data matches original data with comprehensive comparison and HTML reporting |
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

## 📚 Programming And Commandline Interface

Cleared can be used in two ways: as a **Python programming framework** using its standard Python API, or through its **powerful command-line interface (CLI)**. Both approaches provide full access to all de-identification capabilities.

### Python API

Use Cleared programmatically in your Python code:

```python
import cleared as clr
from cleared.cli.utils import load_config_from_file

# Load configuration
config = load_config_from_file("config.yaml")

# Create engine and run de-identification
engine = clr.ClearedEngine.from_config(config)
results = engine.run()
```

### Command-Line Interface

Use Cleared from the terminal with powerful CLI commands:

```bash
# Run de-identification
cleared run config.yaml

# Generate configuration report
cleared describe config.yaml

# Test configuration with sample data
cleared test config.yaml --rows 50

# Verify de-identification results
cleared verify config.yaml ./reversed -o verify-results.json

# Generate HTML verification report
cleared report-verify verify-results.json -o verification-report.html
```

### Visual HTML Reports

Cleared generates comprehensive HTML reports that make it easy to review configurations and verification results. These visual reports provide detailed insights into your de-identification pipeline:

<div align="center">


<img src="https://github.com/nomaai/cleared/blob/main/img/conf-full.png?raw=true" alt="Config Full Report Snapshot" width="100%">

</div>

The HTML reports include:
- **Configuration Reports** - Visualize your entire de-identification setup with `cleared describe`
- **Verification Reports** - Review verification results with detailed comparison statistics
- **Interactive Navigation** - Easy-to-navigate sections for tables, transformers, and settings

## 📚 Documentation

[Visit Documentation](docs/index.md) - Comprehensive Documentation


## 🛣 Roadmap

| Milestone                                    | Status       |
|---------------------------------------------|--------------|
| Multi-table, Multi-id de-ID                  | ✅ Completed |
| Concept based filtering                      | ✅ Completed |
| OMOP  schema defaults                        | ✅ Completed |
| Date/time & age shifting                     | ✅ Completed |
| LLM PHI scanner                              | ⏳ Planned   |
| Audit Logs                                   | ⏳ Planned   |
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

This project is licensed under the **Apache License 2.0 with Commons Clause restriction**.

The Software is provided under the Apache License 2.0, with an additional restriction that prohibits:
- **Selling** the Software (including licensing, distributing for a fee, or deriving commercial advantage)
- **Offering the Software as a Service (SaaS)** (including hosted, cloud, or web-based services where the Software is the primary function)


This restriction does **not** apply to:
- Internal use within your organization
- Research, educational, or non-commercial purposes
- Contributing modifications back to the Software
- Integrating the Software into commercial products where it's not the primary value proposition

For full license terms, see [LICENSE](LICENSE). For commercial licensing options, please contact the copyright holder.

> ⚠️ Disclaimer: This library is provided "as is" without warranty of any kind. It is not a certified compliance tool. You are responsible for validating its use in regulated or clinical environments. 

**Read detailed disclaimers [here](DISCLAIMER.md)**



---

## 🌐 Links

- [📖 Documentation](https://cleared.readthedocs.io)
- [📦 PyPI Package](https://pypi.org/project/cleared)
- [📊 Demo Notebooks](https://github.com/nomaai/cleared/examples)
- [💬 Issues & Discussions](https://github.com/nomaai/cleared/issues)

