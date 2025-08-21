# Batch Analysis Manager

A lightweight, experiment-agnostic batch job runner for particle physics and other data-heavy workflows.  

This tool automates running the same analysis script across many datasets, handles retries, logs output, and can merge results — without needing heavy cluster software like HTCondor or SLURM.

---

## Features
- Run **any** script or command on many datasets automatically
- Parallel execution on your local machine
- Automatic retries for failed jobs
- Per-job log files
- Resume from where you left off
- CSV and (optional) ROOT histogram aggregation
- Simple YAML configuration
- Works with Python scripts, C++ programs, ROOT macros, etc.

---

## Quickstart

### 1. Clone this repository
```bash
git clone https://github.com/yourusername/batch-analysis-manager.git
cd batch-analysis-manager

### 2. Install dependencies
pip install -r requirements.txt
# Optional for ROOT:
pip install uproot

### 3. Prepare datasets
data/
├── sampleA.root
└── sampleB.root

### 4. Create a config.yaml  file
# Example
```command_template: "python run_analysis.py {input} --out {output} --cut {param_cut}"
base_output_dir: "results"
parallel_jobs: 4
retry_failed: 2

parameters:
  cut: 20

datasets:
  - path: "data/sampleA.root"
    name: "sampleA"
  - path: "data/sampleB.root"
    name: "sampleB"

### 5. Run the batch
python batch_manager.py --config config.yaml

## Example Project Structure
batch-analysis-manager/
├── batch_manager.py
├── requirements.txt
├── config.yaml
├── run_analysis.py
└── data/
    ├── sampleA.root
    └── sampleB.root
