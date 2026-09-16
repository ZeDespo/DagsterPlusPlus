"""
An IO Manager is responsible for storing the output value of some asset / op, and
loading it into a downstream asset / op as input.

This file contains the basic definition to be used in all of dagster_plus_plus.
"""

import pickle
from base64 import b64decode, b64encode
from typing import Any

import attrs
import dagster as dag
from returns.maybe import Maybe, Nothing, Some
from returns.result import Failure, ResultE, Success

from dagster_plus_plus.core.data_store import (
    BaseIODataStore,
    IOKey,
)


def _decode_base64_string_to_object(s: str) -> Any:
    """
    Transform whatever was previously encoded back into it's original form. Will
    be the opposite of the encode function.
    """
    return pickle.loads(b64decode(s))


def _encode_object_to_base64_string(value: Any) -> str:
    """
    Standardize whatever gets returned from an op / asset / graph into a
    base64 string, so we can easily store it in whatever data store we're
    using.
    """
    return b64encode(pickle.dumps(value)).decode()


class DagPlusPlusIOManager(dag.ConfigurableIOManager):
    """
    The Dagster IO manager utilizes a file based approach, which isn't very scalable
    if we're doing multiple concurrent runs. It's also extremely strict in terms of
    inputs / outputs.

    This class will facilitate as an external data store-backed IO manager that allows
    for the following functionalities:

        - Loading the output of a partitioned asset's values into a non-partitioned
          one by loading the values in as a sequence.
        - Loading a partitioned-asset's values into a
          non-partitioned asset as a list object
    """

    io_data_store: dag.ResourceDependency[BaseIODataStore]

    def _derive_key_for_loading_upstream_asset(
        self, context: dag.InputContext
    ) -> IOKey | list[IOKey]:
        """
        For loading input, we need to take in a bunch of different variables.

        - Are we loading in a (non-) paritioned asset as input from a (non-)partitioned
          asset's output?
        - Are we dealing with an op in one stage of a graph asset's pipeline?
        - Is the key two dimensional?
        - Do both partition keys match for the input / output asset?
        """
        partition_key = context.partition_key if context.has_partition_key else ""
        upstream_output: dag.OutputContext = context.upstream_output  # type: ignore
        if context.has_asset_key:
            context.log.debug(f"Loading dependency for {context.asset_key!r}")
            if context.has_asset_partitions:
                context.log.debug(
                    f"Dependency is partitioned with {context.asset_partitions_def!r}"
                )
                if partition_key:
                    context.log.debug(f"Run partitioned as {context.partition_key = }")
                else:
                    context.log.debug("Collecting all partitions.")
                    if isinstance(
                        context.asset_partitions_def, dag.DynamicPartitionsDefinition
                    ):
                        upstream_pks = context.instance.get_dynamic_partitions(
                            context.asset_partitions_def.name
                        )
                    else:
                        upstream_pks = context.asset_partitions_def.get_partition_keys()
                    context.log.debug(
                        f"Collected {len(upstream_pks)} partitions from asset."
                    )
                    return [
                        IOKey.generate_key_for_asset(context.asset_key, pk)
                        for pk in upstream_pks
                    ]
            else:
                context.log.debug("Pulling from a non-partitioned asset.")
                partition_key = ""
            return IOKey.generate_key_for_asset(context.asset_key, partition_key)
        return IOKey.generate_key_for_op_in_graph_asset(
            upstream_output, upstream_output.op_def, partition_key
        )

    def _handle_read_from_data_store(
        self, context: dag.InputContext, key: IOKey
    ) -> ResultE[Any]:
        context.log.debug(f"Loading input for {key = }")
        value = self._read_value_from_data_store(key)
        context.log.debug(f"Read {value = }")
        match value:
            case Success(value):
                return Success(value)
            case Failure(e):
                context.log.debug(f"Could not get value from data store: {e}")
                match self._parse_two_dimensional_key_if_applicable(context, key):
                    case Some(value):
                        return Success(value)
        return value  # returns the failed state.

    def _parse_two_dimensional_key_if_applicable(
        self, context: dag.InputContext, key: IOKey
    ) -> Maybe[Any]:
        """
        For all we know, Dagster can change how multi-dimensional keys are represented
        in a drastic way. It's best to delegate this wacky logic to its own method.
        """
        if context.upstream_output.has_partition_key and "|" in key.partition_key:
            split_key = key.partition_key.split("|")
            context.log.debug(
                f"Found 2D partition key, attempting to derive input from {split_key}"
            )
            inputs = [None, None]
            for i in range(2):
                new_key = attrs.evolve(
                    key, partition_key=f"{split_key[0]}|{split_key[i + 1]}"
                )
                inputs[i] = self._read_value_from_data_store(new_key).value_or(None)
                context.log.debug(
                    f"Partition {new_key.partition_key = }; value = {inputs[i]}"
                )
            if None not in inputs:
                raise RuntimeError(
                    f"Cannot resolve which input to load! Found a value for "
                    f"both {split_key[0]}|{split_key[1]} and "
                    f"{split_key[0]}|{split_key[2]}. Consider reformulating "
                    f"your partitions for the asset."
                )
            return Maybe.from_optional(next(filter(bool, inputs), None))
        return Nothing

    def _read_value_from_data_store(self, key: IOKey) -> ResultE[Any]:
        get_result = self.io_data_store.read(key)
        if encoded_string := get_result.value_or(None):
            return Success(_decode_base64_string_to_object(encoded_string))
        return get_result  # Return the Failure object.

    def handle_output(self, context: dag.OutputContext, obj: Any) -> None:
        """
        Save some upstream asset / op's returned value to the data storage object.
        You'll never call this on your own. This is what Dagster needs to make sure
        returned asset values persist across runs.
        """
        key = IOKey.generate_key_from_output_context(context)
        metadata = {}
        if context.output_metadata:
            metadata = {
                key: value.value for key, value in context.output_metadata.items()
            }
        context.log.debug(f"Writing output to {key = }")
        self.io_data_store.write(key, _encode_object_to_base64_string(obj), metadata)
        context.log.debug(f"Written object: {obj}")
        if metadata:
            context.log.debug(f"With metdata: {metadata}")

    def load_input(self, context: dag.InputContext) -> Any:
        """
        Get the raw object from the data store and load into some downstream op / asset.

        Also allows for one to:
            - load partitioned assets into non-partitioned ones.
            - Load non-partitioned assets into partitioned ones.


        Again, you'll never call this on your own. It's for dagster.
        """
        upstream_context = context.upstream_output
        if not upstream_context:
            raise RuntimeError(
                "For some reason, we're loading input when no input is expected."
            )
        context.log.debug(f"Loading output for {upstream_context!r}.")
        key = self._derive_key_for_loading_upstream_asset(context)
        if isinstance(key, IOKey):
            return self._handle_read_from_data_store(context, key).unwrap()
        if isinstance(key, list):
            context.log.debug(
                f"Returning maximum of {len(key)} values from partitioned asset."
            )
            values = dict[str, Any]()  # Key == partition key.
            for k in key:
                match self._handle_read_from_data_store(context, k):
                    case Success(value):
                        values[k.partition_key] = value
                    case Failure(_):
                        context.log.warning(
                            f"Upstream asset {k!r} has not materialized. Downstream"
                            "asset might have incomplete data."
                        )
            return values
        raise TypeError(f"Cannot handle {type(key) = }")
