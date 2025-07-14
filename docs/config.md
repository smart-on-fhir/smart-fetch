---
title: Configuration
parent: SMART Fetch
nav_order: 20
# audience: non-programmers vaguely familiar with the project
# type: how-to
---

# Make Your Life Easier with Config Files

Passing arguments like `--fhir-url` or `--smart-key` every time you run SMART Fetch gets old fast.

Instead, you can set up a config file for your environment.
Then you just pass that config file to SMART Fetch like so: `smart-fetch export -c config.toml`.

## File Format

A SMART Fetch config file is a [TOML](https://toml.io/en/) file,
which will look familiar to anyone that's written an INI file before.

Every key is interpreted as a CLI argument, just without the `--` in front.
For example, these two command lines are equivalent:

```shell
smart-fetch export --fhir-url https://ehr.example/ --group Group1 ./output
smart-fetch export -c config.toml ./output
```

where `config.toml` contains:

```toml
fhir-url = "https://ehr.example/"
group = "Group1"
```

## Inline SMART Keys

You may have a SMART key file that you normally point to on the command line,
like so: `--smart-key keys.jwks`.

When using a config file, you can either continue pointing to the external file or inline the keys
for convenience. That is, these are both valid config files:

```toml
smart-key = "keys.jwks"
```

```toml
smart-key = '{"keys": [{ "kty": "EC", "crv": "P-384", "d": "aaa", "x": "bbb", "y": "ccc", "key_ops": [ "sign" ], "ext": true, "kid": "ddd", "alg": "ES384" }]}'
```

The latter inline approach might be more convenient,
since you'll have a single file with everything you need.
