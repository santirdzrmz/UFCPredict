# UFCPredict

A machine learning project that predicts UFC fight outcomes using historical fight data and fighter statistics scraped from [ufcstats.com](http://www.ufcstats.com).

## Overview

UFCPredict combines web scraping, feature engineering, and gradient boosting to forecast UFC fight winners. It uses a Glicko2 rating system (similar to chess rankings) to capture fighter skill progression over time, and applies XGBoost with Bayesian hyperparameter optimization for predictions. SHAP values provide interpretability into what drives each prediction.

## Features

- **Web scraping** of 8,500+ historical UFC fights and 4,400+ fighter records from ufcstats.com
- **Smart caching** with MD5-hashed filenames and rate limiting (exponential backoff on 429s)
- **14+ engineered difference features**: height, reach, age, strike accuracy, takedown metrics, submission averages, win percentage, etc.
- **Glicko2 rating system** to track fighter skill evolution chronologically
- **XGBoost model** tuned via Bayesian optimization (Gaussian Process, 50 iterations)
- **Logistic Regression baseline** for comparison
- **SHAP explainability** for feature importance analysis
- **Monte Carlo simulation** with noise injection to produce probabilistic fight predictions

## Project Structure

```text
UFCPredict/
├── UFCPrediction.ipynb       # Main analysis, modeling, and prediction notebook
├── fight_stat_scraper.py     # Scrapes fight-level stats → ufc_fight_data.csv
├── fighter_stat_scraper.py   # Scrapes fighter bios/career stats → ufc_fighter_stats.csv
├── ufc_fight_data.csv        # 8,537 fight records
├── ufc_fighter_stats.csv     # 4,455 fighter records
├── environment.yml           # Conda environment specification
└── cache_html/               # Cached HTML pages (reduces repeat network requests)
```

## Setup

### Prerequisites

[Conda](https://docs.conda.io/en/latest/) is required to manage the environment.

### Install

```bash
conda env create -f environment.yml
conda activate ufcpredict
```

The environment includes Python 3.12 and the following key packages:

| Package | Purpose |
| --- | --- |
| `pandas`, `numpy` | Data manipulation |
| `scikit-learn` | Preprocessing, baseline model, metrics |
| `xgboost` | Main predictive model |
| `scikit-optimize` | Bayesian hyperparameter optimization |
| `glicko2` | Fighter skill rating system |
| `shap` | Model explainability |
| `beautifulsoup4`, `requests` | Web scraping |
| `matplotlib` | Visualization |
| `jupyter` | Notebook environment |

## Usage

### 1. Scrape Data

Run the scrapers to fetch the latest UFC data:

```bash
python fight_stat_scraper.py     # → ufc_fight_data.csv
python fighter_stat_scraper.py   # → ufc_fighter_stats.csv
```

The scrapers cache pages in `cache_html/` so re-runs are fast. Cached fighter listing pages are always re-fetched to pick up new records.

### 2. Train and Predict

Open and run `UFCPrediction.ipynb` in Jupyter:

```bash
jupyter notebook UFCPrediction.ipynb
```

The notebook walks through:

1. **Data loading & cleaning** — merges fight and fighter CSVs, normalizes names, parses dates and physical stats
2. **Feature engineering** — computes pairwise difference features and Glicko2 ratings per fighter
3. **Model training** — Logistic Regression baseline + XGBoost with Bayesian optimization
4. **Evaluation** — 5-fold cross-validation, confusion matrix, ROC/AUC curves
5. **Prediction** — `predict_match(fighter_a, fighter_b)` returns win probabilities; Monte Carlo simulation adds variance for realistic uncertainty

## Model Details

| Component | Detail |
| --- | --- |
| Baseline | Logistic Regression (scaled features) |
| Main model | XGBoost (histogram tree method) |
| Hyperparameter search | Bayesian Optimization — 8 random starts + 50 iterations over 7 parameters |
| Cross-validation | 5-fold stratified |
| Explainability | SHAP TreeExplainer |

Key tuned hyperparameters: `max_depth`, `learning_rate`, `n_estimators`, `subsample`, `colsample_bytree`, `gamma`, `min_child_weight`.

## Data Source

All data is scraped from [ufcstats.com](http://www.ufcstats.com). Please be respectful of the site's resources — the scrapers include rate limiting and caching for this reason.
