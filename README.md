# tap-talkable

`tap-talkable` is a Singer tap for Talkable.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Installation

```bash
pipx install git+https://github.com/gthesheep/tap-talkable
```

## Configuration

### Accepted Config Options

`api_key` - Authentication key obtained from Talkable Account Settings.  
`site_slug` - The slug for your site.  
`start_date` - Timestamp for when to collect metrics from, default is `2019-01-01T00:00:00Z`.  

Metrics descriptions can be found using this URL pattern: https://admin.talkable.com/account/<account_id>/metrics

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-talkable --about
```

### Source Authentication and Authorization

See [source docs](https://docs.talkable.com/api_v2/intro.html#authentication)

## Usage

You can easily run `tap-talkable` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-talkable --version
tap-talkable --help
tap-talkable --config CONFIG --discover > ./catalog.json
```

## Developer Resources

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_talkable/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-talkable` CLI interface directly using `poetry run`:

```bash
poetry run tap-talkable --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any _"TODO"_ items listed in
the file.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-talkable
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-talkable --version
# OR run a test `elt` pipeline:
meltano elt tap-talkable target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to 
develop your own taps and targets.
