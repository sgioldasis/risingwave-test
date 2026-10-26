"""The fakes in tests/fakes.py must keep matching the real connection surfaces in
reverse_etl_connections.py.

A fake that drifts (a method renamed or added on the real surface, a parameter changed) would let a test pass
against code that fails live. These tests check every public method of each real surface exists on its fake and
accepts the same parameters. They do not check the other direction: a fake may offer more than the real one.

Run from the repo root:  .venv/bin/python -m pytest
"""

import inspect

import pytest

from orchestration.assets import reverse_etl_connections as connections
from tests import fakes

PAIRS = [
    (type(connections.state_postgres()), fakes.FakeDatabase),
    (type(connections.risingwave()), fakes.FakeDatabase),
    (connections.DatabricksApi, fakes.FakeDatabricks),
    (connections.KafkaClients, fakes.FakeKafka),
    (connections.SchemaRegistry, fakes.FakeSchemaRegistry),
    (connections.Http, fakes.FakeHttp),
]
IDS = [f"{real.__name__}-{fake.__name__}" for real, fake in PAIRS]


def public_methods(cls) -> dict:
    """The methods the connection surface itself defines."""
    return {name: fn for name, fn in vars(cls).items() if inspect.isfunction(fn) and not name.startswith("_")}


def parameters(fn) -> dict:
    return {name: p for name, p in inspect.signature(fn).parameters.items() if name != "self"}


@pytest.mark.parametrize("real, fake", PAIRS, ids=IDS)
def test_the_fake_has_every_method_of_the_real_connection(real, fake):
    missing = sorted(set(public_methods(real)) - set(dir(fake)))
    assert not missing, f"{fake.__name__} lacks {missing} of {real.__name__}"


@pytest.mark.parametrize("real, fake", PAIRS, ids=IDS)
def test_the_fake_accepts_the_same_parameters_as_the_real_method(real, fake):
    for name, real_method in public_methods(real).items():
        fake_params = parameters(getattr(fake, name))
        accepts_any_keyword = any(p.kind is inspect.Parameter.VAR_KEYWORD for p in fake_params.values())
        for param_name, param in parameters(real_method).items():
            if param.kind is inspect.Parameter.VAR_KEYWORD:
                assert accepts_any_keyword, f"{fake.__name__}.{name} must accept **{param_name}"
            else:
                assert param_name in fake_params or accepts_any_keyword, (
                    f"{fake.__name__}.{name} does not accept {param_name!r}"
                )
        required_by_fake = {
            n for n, p in fake_params.items() if p.default is inspect.Parameter.empty and p.kind is p.POSITIONAL_OR_KEYWORD
        }
        assert required_by_fake <= set(parameters(real_method)), (
            f"{fake.__name__}.{name} requires {sorted(required_by_fake - set(parameters(real_method)))} "
            f"that {real.__name__}.{name} does not take"
        )
