"""Tests for the message-model capability."""

from __future__ import annotations

import uuid

import pytest

from pactix.errors import ValidationError
from pactix.message import MessageEnvelope, MessageMetadata


def test_construct_with_explicit_message_id() -> None:
    meta = MessageMetadata('m-1', 'order.created')
    assert meta.message_id == 'm-1'
    assert meta.event_type == 'order.created'
    assert meta.message_key is None
    assert meta.ordering_key is None
    assert meta.correlation_id is None
    assert meta.headers == {}
    assert meta.occurred_at is not None


def test_construct_with_generated_message_id() -> None:
    meta = MessageMetadata.for_event_type('order.created')
    # parses as a valid UUID
    uuid.UUID(meta.message_id)
    assert meta.event_type == 'order.created'


def test_builders_are_immutable_and_set_fields() -> None:
    base = MessageMetadata('m-1', 'order.created')
    built = base.with_message_key('k').with_ordering_key('o').with_correlation_id('c')
    assert (built.message_key, built.ordering_key, built.correlation_id) == ('k', 'o', 'c')
    # original unchanged
    assert base.message_key is None


def test_reject_empty_message_id() -> None:
    with pytest.raises(ValidationError, match='message_id must not be empty'):
        MessageMetadata('  ', 'order.created').validate()


def test_reject_empty_event_type() -> None:
    with pytest.raises(ValidationError, match='event_type must not be empty'):
        MessageMetadata('m-1', '').validate()


def test_require_ordering_key_when_required() -> None:
    with pytest.raises(ValidationError, match='ordering_key is required'):
        MessageMetadata('m-1', 'order.created').validate(requires_ordering_key=True)


def test_reject_blank_ordering_key_when_provided() -> None:
    with pytest.raises(ValidationError, match='ordering_key must not be empty when provided'):
        MessageMetadata('m-1', 'order.created', ordering_key='  ').validate()


def test_valid_envelope() -> None:
    env = MessageEnvelope(MessageMetadata('m-1', 'order.created'), {'a': 1})
    assert env.payload == {'a': 1}
    assert env.metadata.message_id == 'm-1'


def test_envelope_rejects_invalid_metadata() -> None:
    with pytest.raises(ValidationError):
        MessageEnvelope(MessageMetadata('m-1', ''), {'a': 1})
