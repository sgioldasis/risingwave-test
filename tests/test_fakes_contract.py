"""The fakes in tests/fakes.py must keep matching the real resources in reverse_etl_resources.py.

A fake that drifts (a method renamed or added on the real resource, a parameter changed) would let a test pass
against code that fails live. These tests check every public method of each real resource exists on its fake and
accepts the same parameters. They do not check the other direction: a fake may offer more than the real one.

Run from the repo root:  .venv/bin/python -m pytest
"""

import inspect

import pytest

from orchestration.assets import reverse_etl_resources as real
from tests import fakes

PAIRS = [
    (real.PostgresResource, fakes.FakeDatabase),
    (real.RisingWaveResource, fakes.FakeDatabase),
    (real.DatabricksResource, fakes.FakeDatabricks),
    (real.KafkaResource, fakes.FakeKafka),
    (real.SchemaRegistryResource, fakes.FakeSchemaRegistry),
    (real.HttpResource, fakes.FakeHttp),
]
IDS = [f"{resource.__name__}-{fake.__name__}" for resource, fake in PAIRS]


def public_methods(cls) -> dict:
    """The methods the resource class itself defines (not what ConfigurableResource inherits)."""
    return {name: fn for name, fn in vars(cls).items() if inspect.isfunction(fn) and not name.startswith("_")}


def parameters(fn) -> dict:
    return {name: p for name, p in inspect.signature(fn).parameters.items() if name != "self"}


@pytest.mark.parametrize("resource, fake", PAIRS, ids=IDS)
def test_the_fake_has_every_method_of_the_real_resource(resource, fake):
    missing = sorted(set(public_methods(resource)) - set(dir(fake)))
    assert not missing, f"{fake.__name__} lacks {missing} of {resource.__name__}"


@pytest.mark.parametrize("resource, fake", PAIRS, ids=IDS)
def test_the_fake_accepts_the_same_parameters_as_the_real_method(resource, fake):
    for name, real_method in public_methods(resource).items():
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
            f"that {resource.__name__}.{name} does not take"
        )
