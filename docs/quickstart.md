# Quickstart Guide

This guide covers running the de-identification pipeline on your data.

## Overview

Before running on your data:
1. ✅ Install Cleared (see [Installation](#installation))
2. ✅ Validate configuration (see [Step 2: Validate Configuration](#step-2-validate-configuration))
3. ✅ Test on sample data (see [Step 4: Test Run](#step-4-test-run))

The workflow consists of eight main steps:

1. **Run Setup** - Create all required directories
2. **Validate Configuration** - Verify configuration is correct
3. **Prepare Input Data** - Place your data files
4. **Test Run** - Dry-run with limited data to verify configuration
5. **Run De-identification** - De-identify the data
6. **Reverse De-identification** - Reverse the de-identification to verify reversibility
7. **Verify Results** - Compare original and reversed data
8. **Generate Report** - Create an HTML report of verification results

## Installation

Make sure you have Cleared installed:

```bash
pip install cleared
```

## Step 1: Run Setup

```bash
cleared setup config.yaml
```

This command creates all required directories specified in your configuration file.

## Step 2: Validate Configuration

```bash
cleared validate config.yaml
```

This validates your configuration file syntax and checks for common issues.

## Step 3: Prepare Input Data

Place your CSV files in the input directory specified in your configuration file. The file names should match the table names defined in your configuration.

For example, if your configuration defines a table named `users`, place your data file as `users.csv` in the input directory.

## Step 4: Test Run

```bash
cleared test config.yaml --rows 50
```

This performs a dry run that processes only the first 50 rows of each table and does not write any outputs, making it safe to test your configuration.

## Step 5: Run De-identification

```bash
cleared run config.yaml
```

This runs the full de-identification pipeline on your data. The de-identified data will be written to the output directory, and de-identification mappings will be saved to the deid_ref directory.

## Step 6: Reverse De-identification

```bash
cleared reverse config.yaml --output ./reversed
```

This reverses the de-identification using the saved mappings, allowing you to verify that the process is reversible.

## Step 7: Verify Results

```bash
cleared verify config.yaml ./reversed -o verify-results.json
```

This compares the original data with the reversed data to ensure they match, generating a JSON file with detailed verification results.

## Step 8: Generate Report

```bash
cleared report-verify verify-results.json -o verification-report.html
```

This generates a comprehensive HTML report of the verification results, making it easy to review the comparison between original and reversed data.

## Next Tutorial

Continue to the next tutorial: [Single Table Example](use_cleared_config.md) - De-identification with YAML configs and CLI
