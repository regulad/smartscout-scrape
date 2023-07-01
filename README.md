# Smartscout-scrape

[![Version status](https://img.shields.io/pypi/status/smartscout-scrape)](https://pypi.org/project/smartscout-scrape)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python version compatibility](https://img.shields.io/pypi/pyversions/smartscout-scrape)](https://pypi.org/project/smartscout-scrape)
[![Version on Docker Hub](https://img.shields.io/docker/v/regulad/smartscout-scrape?color=green&label=Docker%20Hub)](https://hub.docker.com/repository/docker/regulad/smartscout-scrape)
[![Version on GitHub](https://img.shields.io/github/v/release/regulad/smartscout-scrape?include_prereleases&label=GitHub)](https://github.com/regulad/smartscout-scrape/releases)
[![Version on PyPi](https://img.shields.io/pypi/v/smartscoutscrape)](https://pypi.org/project/smartscoutscrape)
[![Version on Conda-Forge](https://img.shields.io/conda/vn/conda-forge/smartscout-scrape?label=Conda-Forge)](https://anaconda.org/conda-forge/smartscout-scrape)
[![Documentation status](https://readthedocs.org/projects/smartscout-scrape/badge)](https://smartscout-scrape.readthedocs.io/en/stable)
[![Build (GitHub Actions)](https://img.shields.io/github/workflow/status/regulad/smartscout-scrape/Build%20&%20test?label=Build%20&%20test)](https://github.com/regulad/smartscout-scrape/actions)
[![Build (Travis)](https://img.shields.io/travis/regulad/smartscout-scrape?label=Travis)](https://travis-ci.com/regulad/smartscout-scrape)
[![Build (Azure)](https://img.shields.io/azure-devops/build/regulad/<<key>>/<<defid>>?label=Azure)](https://dev.azure.com/regulad/smartscout-scrape/_build?definitionId=1&_a=summary)
[![Build (Scrutinizer)](https://scrutinizer-ci.com/g/regulad/smartscout-scrape/badges/build.png?b=main)](https://scrutinizer-ci.com/g/regulad/smartscout-scrape/build-status/main)
[![Test coverage (coveralls)](https://coveralls.io/repos/github/regulad/smartscout-scrape/badge.svg?branch=main&service=github)](https://coveralls.io/github/regulad/smartscout-scrape?branch=main)
[![Test coverage (codecov)](https://codecov.io/github/regulad/smartscout-scrape/coverage.svg)](https://codecov.io/gh/regulad/smartscout-scrape)
[![Test coverage (Scrutinizer)](https://scrutinizer-ci.com/g/regulad/smartscout-scrape/badges/coverage.png?b=main)](https://scrutinizer-ci.com/g/regulad/smartscout-scrape/?branch=main)
[![Maintainability (Code Climate)](https://api.codeclimate.com/v1/badges/<<apikey>>/maintainability)](https://codeclimate.com/github/regulad/smartscout-scrape/maintainability)
[![CodeFactor](https://www.codefactor.io/repository/github/dmyersturnbull/tyrannosaurus/badge)](https://www.codefactor.io/repository/github/dmyersturnbull/tyrannosaurus)
[![Code Quality (Scrutinizer)](https://scrutinizer-ci.com/g/regulad/smartscout-scrape/badges/quality-score.png?b=main)](https://scrutinizer-ci.com/g/regulad/smartscout-scrape/?branch=main)
[![Created with Tyrannosaurus](https://img.shields.io/badge/Created_with-Tyrannosaurus-0000ff.svg)](https://github.com/dmyersturnbull/tyrannosaurus)

Lightweight data-extraction & general use library for smartscout.com.

## Proxy

Included is a simple Tailscale configuration that serves a SOCKS5 proxy on your local machine.

Copy the contents of `.env-example` into `.env`, populate the fields and then perform the following command on a machine that has docker installed:

```bash
docker-compose up -d
```

Then, you can use the `--proxy` flag to use the proxy.

```bash
poetry run amzscout-scrape --proxy socks5://localhost:1055
```

And it’s further described in this paragraph.
[See the docs 📚](https://smartscout-scrape.readthedocs.io/en/stable/) for more info.

Licensed under the terms of the [Apache License 2.0](https://spdx.org/licenses/Apache-2.0.html).
[New issues](https://github.com/regulad/smartscout-scrape/issues) and pull requests are welcome.
Please refer to the [contributing guide](https://github.com/regulad/smartscout-scrape/blob/main/CONTRIBUTING.md)
and [security policy](https://github.com/regulad/smartscout-scrape/blob/main/SECURITY.md).
Generated with [Tyrannosaurus](https://github.com/dmyersturnbull/tyrannosaurus).
