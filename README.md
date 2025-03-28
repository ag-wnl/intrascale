# Intrascale

A distributed computing framework that allows devices on the same network to pool their hardware resources and use them as a unified system.

## Features

- Automatic device discovery on local network
- Hardware resource pooling
- Distributed computing capabilities

## Installation

1. Clone the repository and open:

```bash
cd intrascale
```

2. Create a virtual environment (recommended):

```bash
python -m venv venv
source venv/bin/activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

## Usage

Run the discovery service:

```bash
python -m intrascale.discovery
```

## Development

- Run tests: `pytest`
- Format code: `black .`
- Lint code: `flake8`
