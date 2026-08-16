"""Hadoop-style Iceberg catalog: warehouse is the lake root, pointer is version-hint.

PyIceberg 0.9+ dropped HadoopCatalog. DET keeps the same filesystem contract:
table data+metadata live at an explicit location (``{lake}/bronze/{provider}/{source}_vN``),
and ``metadata/version-hint.text`` is the current metadata file. No Glue/REST/SQLite.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyiceberg.catalog import (
    EMPTY_DICT,
    WAREHOUSE_LOCATION,
    Catalog,
    Identifier,
    MetastoreCatalog,
    Properties,
    PropertiesUpdateSummary,
)
from pyiceberg.exceptions import (
    CommitFailedException,
    NoSuchTableError,
    NoSuchViewError,
    TableAlreadyExistsError,
)
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.serializers import FromInputFile
from pyiceberg.table import CommitTableResponse, Table, TableProperties
from pyiceberg.table.locations import load_location_provider
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortOrder
from pyiceberg.table.update import TableRequirement, TableUpdate
from pyiceberg.typedef import Properties as IcebergProperties

if TYPE_CHECKING:
    import pyarrow as pa

_HINT = "metadata/version-hint.text"


class LakeHadoopCatalog(MetastoreCatalog):
    """Filesystem catalog: identifier → table location, location → version-hint."""

    def __init__(self, name: str = "det", **properties: str) -> None:
        super().__init__(name, **properties)
        warehouse = properties.get(WAREHOUSE_LOCATION) or properties.get("warehouse")
        if not warehouse or not str(warehouse).strip():
            raise ValueError("Hadoop Iceberg catalog requires warehouse= (the lake root URI)")
        self._warehouse = str(warehouse).rstrip("/")
        self._locations: dict[tuple[str, ...], str] = {}

    def bind_location(self, identifier: str | Identifier, location: str) -> None:
        self._locations[Catalog.identifier_to_tuple(identifier)] = location.rstrip("/")

    def table_location(self, identifier: str | Identifier) -> str:
        ident = Catalog.identifier_to_tuple(identifier)
        if ident in self._locations:
            return self._locations[ident]
        namespace, table_name = Catalog.identifier_to_database_and_table(identifier)
        return f"{self._warehouse}/{namespace}/{table_name}"

    def _hint_path(self, table_location: str) -> str:
        return f"{table_location.rstrip('/')}/{_HINT}"

    def _read_hint(self, table_location: str) -> str | None:
        io = self._load_file_io(location=table_location)
        path = self._hint_path(table_location)
        inp = io.new_input(path)
        if not inp.exists():
            return None
        with inp.open() as fh:
            raw = fh.read()
        text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
        hint = text.strip()
        return hint or None

    def _write_hint(self, table_location: str, metadata_location: str) -> None:
        io = self._load_file_io(location=table_location)
        path = self._hint_path(table_location)
        with io.new_output(path).create(overwrite=True) as fh:
            fh.write(metadata_location.encode("utf-8"))

    def _table_from_metadata(
        self, identifier: str | Identifier, metadata_location: str
    ) -> Table:
        io = self._load_file_io(location=metadata_location)
        metadata = FromInputFile.table_metadata(io.new_input(metadata_location))
        namespace, table_name = Catalog.identifier_to_database_and_table(identifier)
        ident = Catalog.identifier_to_tuple(namespace) + (table_name,)
        self.bind_location(ident, metadata.location)
        return Table(
            identifier=ident,
            metadata=metadata,
            metadata_location=metadata_location,
            io=self._load_file_io(metadata.properties, metadata_location),
            catalog=self,
        )

    def create_table(
        self,
        identifier: str | Identifier,
        schema: Schema | pa.Schema,
        location: str | None = None,
        partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
        sort_order: SortOrder = UNSORTED_SORT_ORDER,
        properties: IcebergProperties = EMPTY_DICT,
    ) -> Table:
        schema = self._convert_schema_if_needed(
            schema,
            int(
                properties.get(
                    TableProperties.FORMAT_VERSION,
                    TableProperties.DEFAULT_FORMAT_VERSION,
                )
            ),
        )
        namespace, table_name = Catalog.identifier_to_database_and_table(identifier)
        table_location = self._resolve_table_location(location, namespace, table_name)
        self.bind_location(identifier, table_location)
        if self._read_hint(table_location) is not None:
            raise TableAlreadyExistsError(f"Table {namespace}.{table_name} already exists")

        provider = load_location_provider(table_location, properties)
        metadata_location = provider.new_table_metadata_file_location()
        metadata = new_table_metadata(
            location=table_location,
            schema=schema,
            partition_spec=partition_spec,
            sort_order=sort_order,
            properties=properties,
        )
        io = self._load_file_io(properties=properties, location=metadata_location)
        self._write_metadata(metadata, io, metadata_location)
        self._write_hint(table_location, metadata_location)
        return self.load_table(identifier)

    def load_table(self, identifier: str | Identifier) -> Table:
        table_location = self.table_location(identifier)
        metadata_location = self._read_hint(table_location)
        if metadata_location is None:
            namespace, table_name = Catalog.identifier_to_database_and_table(identifier)
            raise NoSuchTableError(f"Table does not exist: {namespace}.{table_name}")
        return self._table_from_metadata(identifier, metadata_location)

    def drop_table(self, identifier: str | Identifier) -> None:
        table_location = self.table_location(identifier)
        if self._read_hint(table_location) is None:
            namespace, table_name = Catalog.identifier_to_database_and_table(identifier)
            raise NoSuchTableError(f"Table does not exist: {namespace}.{table_name}")
        io = self._load_file_io(location=table_location)
        io.delete(self._hint_path(table_location))

    def register_table(self, identifier: str | Identifier, metadata_location: str) -> Table:
        table = self._table_from_metadata(identifier, metadata_location)
        self._write_hint(table.metadata.location, metadata_location)
        return table

    def commit_table(
        self,
        table: Table,
        requirements: tuple[TableRequirement, ...],
        updates: tuple[TableUpdate, ...],
    ) -> CommitTableResponse:
        table_identifier = table.name()
        table_location = table.metadata.location
        self.bind_location(table_identifier, table_location)
        current_table: Table | None
        try:
            current_table = self.load_table(table_identifier)
        except NoSuchTableError:
            current_table = None

        staged = self._update_and_stage_table(current_table, table.name(), requirements, updates)
        if current_table and staged.metadata == current_table.metadata:
            return CommitTableResponse(
                metadata=current_table.metadata,
                metadata_location=current_table.metadata_location,
            )
        if current_table is not None:
            live_hint = self._read_hint(table_location)
            if live_hint != current_table.metadata_location:
                raise CommitFailedException(
                    f"Table has been updated by another process: {table_location}"
                )
        self._write_metadata(staged.metadata, staged.io, staged.metadata_location)
        self._write_hint(table_location, staged.metadata_location)
        return CommitTableResponse(
            metadata=staged.metadata, metadata_location=staged.metadata_location
        )

    def namespace_exists(self, namespace: str | Identifier) -> bool:
        _ = namespace
        return True

    def create_namespace(
        self, namespace: str | Identifier, properties: Properties = EMPTY_DICT
    ) -> None:
        _ = namespace, properties

    def drop_namespace(self, namespace: str | Identifier) -> None:
        _ = namespace

    def list_tables(self, namespace: str | Identifier) -> list[Identifier]:
        _ = namespace
        return []

    def list_namespaces(self, namespace: str | Identifier = ()) -> list[Identifier]:
        _ = namespace
        return []

    def load_namespace_properties(self, namespace: str | Identifier) -> Properties:
        _ = namespace
        return {}

    def update_namespace_properties(
        self,
        namespace: str | Identifier,
        removals: set[str] | None = None,
        updates: Properties = EMPTY_DICT,
    ) -> PropertiesUpdateSummary:
        _ = namespace, removals, updates
        return PropertiesUpdateSummary(removed=[], updated=[], missing=[])

    def rename_table(
        self, from_identifier: str | Identifier, to_identifier: str | Identifier
    ) -> Table:
        raise NotImplementedError("Hadoop Iceberg catalog does not rename tables")

    def list_views(self, namespace: str | Identifier) -> list[Identifier]:
        _ = namespace
        return []

    def drop_view(self, identifier: str | Identifier) -> None:
        raise NoSuchViewError(f"View does not exist: {identifier}")

    def view_exists(self, identifier: str | Identifier) -> bool:
        _ = identifier
        return False
